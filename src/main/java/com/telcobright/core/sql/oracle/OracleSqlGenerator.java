package com.telcobright.core.sql.oracle;

import com.telcobright.core.sql.BaseSqlGenerator;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Oracle SQL Generator
 * Optimized SQL generation for Oracle database with support for:
 * - Interval and reference partitioning
 * - Oracle-specific optimizations
 * - Advanced features like flashback queries
 */
public class OracleSqlGenerator extends BaseSqlGenerator {

    @Override
    public String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                                   String partitionColumn, String partitionType) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(escapeIdentifier(tableName)).append(" (\n");

        // Add columns
        List<FieldMetadata> fields = metadata.getFields();
        for (int i = 0; i < fields.size(); i++) {
            FieldMetadata field = fields.get(i);
            sql.append("  ").append(escapeIdentifier(field.getColumnName()))
               .append(" ").append(getSqlType(field));

            if (field.equals(metadata.getIdField())) {
                sql.append(" PRIMARY KEY");
            }

            if (i < fields.size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        sql.append(")\n");

        // Oracle partitioning
        if ("RANGE".equalsIgnoreCase(partitionType)) {
            sql.append("PARTITION BY RANGE (");
            FieldMetadata partitionField = getFieldByColumnName(metadata, partitionColumn);
            if (partitionField != null && partitionField.getType() == java.time.LocalDateTime.class) {
                sql.append(escapeIdentifier(partitionColumn));
            } else {
                sql.append(escapeIdentifier(partitionColumn));
            }
            sql.append(")\n");

            // Add interval partitioning for automatic partition creation
            if (partitionField != null && partitionField.getType() == java.time.LocalDateTime.class) {
                sql.append("INTERVAL (NUMTODSINTERVAL(1, 'DAY'))\n");
                sql.append("(\n");
                sql.append("  PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')\n");
                sql.append(")");
            }
        } else if ("HASH".equalsIgnoreCase(partitionType)) {
            sql.append("PARTITION BY HASH (").append(escapeIdentifier(partitionColumn)).append(")\n");
            sql.append("PARTITIONS 16"); // Default to 16 hash partitions
        } else {
            sql.append("PARTITION BY LIST (").append(escapeIdentifier(partitionColumn)).append(")");
        }

        return sql.toString();
    }

    @Override
    public String generateAlterTableAddPartition(String tableName, String partitionName,
                                                String partitionDefinition) {
        return String.format("ALTER TABLE %s ADD PARTITION %s VALUES LESS THAN (%s)",
                escapeIdentifier(tableName),
                escapeIdentifier(partitionName),
                partitionDefinition);
    }

    @Override
    public String generateDropTable(String tableName) {
        // Oracle supports CASCADE CONSTRAINTS
        return "DROP TABLE " + escapeIdentifier(tableName) + " CASCADE CONSTRAINTS";
    }

    @Override
    public String generateCreateIndex(String tableName, String indexName, List<String> columns) {
        String columnList = columns.stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        // Oracle supports local and global indexes for partitioned tables
        return String.format("CREATE INDEX %s ON %s (%s) LOCAL",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }

    @Override
    public String generateBatchInsert(String tableName, List<Map<String, Object>> records) {
        // Oracle supports INSERT ALL for batch operations
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate batch insert for empty records");
        }

        Map<String, Object> firstRecord = records.get(0);
        String columns = firstRecord.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder("INSERT ALL\n");

        for (Map<String, Object> record : records) {
            String valuePlaceholders = record.keySet().stream()
                    .map(k -> "?")
                    .collect(Collectors.joining(", "));
            sql.append("  INTO ").append(escapeIdentifier(tableName))
               .append(" (").append(columns).append(") VALUES (")
               .append(valuePlaceholders).append(")\n");
        }

        sql.append("SELECT 1 FROM DUAL");
        return sql.toString();
    }

    @Override
    public String generateSelectWithPagination(String baseQuery, int limit, int offset) {
        // Oracle 12c+ supports OFFSET/FETCH syntax
        return baseQuery + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }

    @Override
    public String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                                    String lastId, int batchSize) {
        // Oracle with optimizer hints
        return String.format("SELECT /*+ INDEX(%s PK_%s) */ * FROM %s WHERE %s > ? " +
                           "ORDER BY %s FETCH FIRST %d ROWS ONLY",
                tableName, tableName,
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(idColumn),
                batchSize);
    }

    @Override
    public String generateExistsQuery(String tableName, String idColumn) {
        // Oracle EXISTS with ROWNUM optimization
        return String.format(
            "SELECT CASE WHEN EXISTS(SELECT 1 FROM %s WHERE %s = ? AND ROWNUM = 1) " +
            "THEN 1 ELSE 0 END AS record_exists FROM DUAL",
            escapeIdentifier(tableName),
            escapeIdentifier(idColumn)
        );
    }

    @Override
    public String escapeIdentifier(String identifier) {
        // Oracle uses double quotes for case-sensitive identifiers
        // We'll use uppercase without quotes for standard Oracle naming
        return identifier.toUpperCase();
    }

    @Override
    public String getCurrentTimestamp() {
        // Oracle current timestamp
        return "SYSTIMESTAMP";
    }

    @Override
    public boolean supportsIfNotExists() {
        // Oracle 23c supports IF NOT EXISTS, earlier versions don't
        return false;
    }

    @Override
    public boolean supportsNativePartitioning() {
        return true;
    }

    @Override
    protected String getSqlType(FieldMetadata field) {
        Class<?> type = field.getType();
        String columnName = field.getColumnName();

        if (type == String.class) {
            if (columnName.equals("id")) {
                return "VARCHAR2(255) NOT NULL";
            } else if (columnName.contains("email") || columnName.contains("name")) {
                return "VARCHAR2(255)";
            } else {
                return "CLOB";
            }
        } else if (type == Integer.class || type == int.class) {
            return "NUMBER(10)";
        } else if (type == Long.class || type == long.class) {
            return "NUMBER(19)";
        } else if (type == java.time.LocalDateTime.class) {
            return "TIMESTAMP";
        } else if (type == java.time.LocalDate.class) {
            return "DATE";
        } else if (type == java.time.LocalTime.class) {
            return "TIMESTAMP";
        } else if (type == Boolean.class || type == boolean.class) {
            return "NUMBER(1)";
        } else if (type == Double.class || type == double.class) {
            return "BINARY_DOUBLE";
        } else if (type == Float.class || type == float.class) {
            return "BINARY_FLOAT";
        } else if (type == java.math.BigDecimal.class) {
            return "NUMBER(19, 4)";
        } else if (type == byte[].class) {
            return "BLOB";
        } else {
            return "CLOB";
        }
    }

    // Oracle-specific methods

    /**
     * Generate SQL for interval partitioning with automatic management
     */
    public String generateIntervalPartitionedTable(String tableName, EntityMetadata metadata,
                                                  String partitionColumn, String interval) {
        StringBuilder sql = new StringBuilder(generateCreateTable(tableName, metadata));

        String baseTable = sql.toString().trim();
        if (baseTable.endsWith(";")) {
            baseTable = baseTable.substring(0, baseTable.length() - 1);
        }

        sql = new StringBuilder(baseTable);
        sql.append("\nPARTITION BY RANGE (").append(escapeIdentifier(partitionColumn)).append(")\n");
        sql.append("INTERVAL (").append(interval).append(")\n");
        sql.append("(\n");
        sql.append("  PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')\n");
        sql.append(")");

        return sql.toString();
    }

    /**
     * Generate SQL for flashback query
     */
    public String generateFlashbackQuery(String tableName, String asOfTimestamp) {
        return String.format("SELECT * FROM %s AS OF TIMESTAMP TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')",
                escapeIdentifier(tableName), asOfTimestamp);
    }

    /**
     * Generate SQL for checking partition information
     */
    public String generateShowPartitions(String tableName) {
        return String.format(
            "SELECT PARTITION_NAME, HIGH_VALUE, PARTITION_POSITION, TABLESPACE_NAME, NUM_ROWS " +
            "FROM USER_TAB_PARTITIONS " +
            "WHERE TABLE_NAME = '%s' " +
            "ORDER BY PARTITION_POSITION",
            tableName.toUpperCase()
        );
    }

    /**
     * Generate MERGE statement for upsert operations
     */
    public String generateMerge(String tableName, Map<String, Object> values, String idColumn) {
        String columns = values.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String updateSetClause = values.keySet().stream()
                .filter(col -> !col.equals(idColumn))
                .map(col -> "t." + escapeIdentifier(col) + " = s." + escapeIdentifier(col))
                .collect(Collectors.joining(", "));

        String valuePlaceholders = values.keySet().stream()
                .map(k -> "?")
                .collect(Collectors.joining(", "));

        return String.format(
            "MERGE INTO %s t " +
            "USING (SELECT %s FROM DUAL) s " +
            "ON (t.%s = s.%s) " +
            "WHEN MATCHED THEN UPDATE SET %s " +
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
            escapeIdentifier(tableName),
            valuePlaceholders,
            escapeIdentifier(idColumn), escapeIdentifier(idColumn),
            updateSetClause,
            columns, valuePlaceholders
        );
    }
}