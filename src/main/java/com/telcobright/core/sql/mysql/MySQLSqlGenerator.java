package com.telcobright.core.sql.mysql;

import com.telcobright.core.sql.BaseSqlGenerator;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MySQL SQL Generator
 * Optimized SQL generation for MySQL database with support for:
 * - Native partitioning (RANGE, LIST, HASH)
 * - MySQL-specific syntax and features
 * - Efficient batch operations
 */
public class MySQLSqlGenerator extends BaseSqlGenerator {

    @Override
    public String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                                   String partitionColumn, String partitionType) {
        StringBuilder sql = new StringBuilder(generateCreateTable(tableName, metadata));

        // Remove the closing semicolon if present
        String baseTable = sql.toString().trim();
        if (baseTable.endsWith(";")) {
            baseTable = baseTable.substring(0, baseTable.length() - 1);
        }

        // Add partitioning clause
        sql = new StringBuilder(baseTable);
        sql.append("\nPARTITION BY RANGE (");

        // Handle different partition column types
        FieldMetadata partitionField = getFieldByColumnName(metadata, partitionColumn);
        if (partitionField != null && partitionField.getType() == java.time.LocalDateTime.class) {
            sql.append("TO_DAYS(").append(escapeIdentifier(partitionColumn)).append(")");
        } else {
            sql.append(escapeIdentifier(partitionColumn));
        }
        sql.append(")");

        return sql.toString();
    }

    @Override
    public String generateAlterTableAddPartition(String tableName, String partitionName,
                                                String partitionDefinition) {
        return String.format("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s))",
                escapeIdentifier(tableName),
                escapeIdentifier(partitionName),
                partitionDefinition);
    }

    @Override
    public String generateCreateIndex(String tableName, String indexName, List<String> columns) {
        String columnList = columns.stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        // MySQL allows index hints and types
        return String.format("CREATE INDEX %s ON %s (%s) USING BTREE",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }

    @Override
    public String generateDropIndex(String tableName, String indexName) {
        // MySQL requires table name in DROP INDEX
        return String.format("DROP INDEX %s ON %s",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName));
    }

    @Override
    public String generateBatchInsert(String tableName, List<Map<String, Object>> records) {
        // MySQL supports efficient multi-row INSERT
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate batch insert for empty records");
        }

        Map<String, Object> firstRecord = records.get(0);
        String columns = firstRecord.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(escapeIdentifier(tableName))
           .append(" (").append(columns).append(") VALUES ");

        for (int i = 0; i < records.size(); i++) {
            String valuePlaceholders = firstRecord.keySet().stream()
                    .map(k -> "?")
                    .collect(Collectors.joining(", "));
            sql.append("(").append(valuePlaceholders).append(")");
            if (i < records.size() - 1) {
                sql.append(", ");
            }
        }

        // Add ON DUPLICATE KEY UPDATE for upsert behavior
        sql.append(" ON DUPLICATE KEY UPDATE ");
        String updateClause = firstRecord.keySet().stream()
                .filter(col -> !col.equals("id")) // Don't update ID
                .map(col -> escapeIdentifier(col) + " = VALUES(" + escapeIdentifier(col) + ")")
                .collect(Collectors.joining(", "));
        sql.append(updateClause);

        return sql.toString();
    }

    @Override
    public String generateSelectWithPagination(String baseQuery, int limit, int offset) {
        // MySQL supports LIMIT with offset
        return baseQuery + " LIMIT " + offset + ", " + limit;
    }

    @Override
    public String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                                    String lastId, int batchSize) {
        // MySQL optimization with index hints
        return String.format("SELECT * FROM %s USE INDEX(PRIMARY) WHERE %s > ? ORDER BY %s LIMIT %d",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(idColumn),
                batchSize);
    }

    @Override
    public String generateExistsQuery(String tableName, String idColumn) {
        // MySQL-optimized EXISTS query
        return String.format("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ? LIMIT 1) as record_exists",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String escapeIdentifier(String identifier) {
        // MySQL uses backticks for identifiers
        return "`" + identifier + "`";
    }

    @Override
    public String getCurrentTimestamp() {
        // MySQL-specific current timestamp
        return "NOW()";
    }

    @Override
    public boolean supportsIfNotExists() {
        return true;
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
                return "VARCHAR(255) NOT NULL";
            } else if (columnName.contains("email")) {
                return "VARCHAR(255)";
            } else if (columnName.contains("name")) {
                return "VARCHAR(100)";
            } else {
                return "TEXT";
            }
        } else if (type == Integer.class || type == int.class) {
            return "INT";
        } else if (type == Long.class || type == long.class) {
            return "BIGINT";
        } else if (type == java.time.LocalDateTime.class) {
            // MySQL DATETIME for LocalDateTime
            return "DATETIME";
        } else if (type == java.time.LocalDate.class) {
            return "DATE";
        } else if (type == java.time.LocalTime.class) {
            return "TIME";
        } else if (type == Boolean.class || type == boolean.class) {
            return "TINYINT(1)";
        } else if (type == Double.class || type == double.class) {
            return "DOUBLE";
        } else if (type == Float.class || type == float.class) {
            return "FLOAT";
        } else if (type == java.math.BigDecimal.class) {
            return "DECIMAL(19, 4)";
        } else if (type == byte[].class) {
            return "BLOB";
        } else {
            return "TEXT";
        }
    }

    // MySQL-specific methods for advanced features

    /**
     * Generate SQL for creating a partitioned table with hourly partitions
     */
    public String generateHourlyPartitionedTable(String tableName, EntityMetadata metadata,
                                                String partitionColumn) {
        StringBuilder sql = new StringBuilder(generateCreateTable(tableName, metadata));

        String baseTable = sql.toString().trim();
        if (baseTable.endsWith(";")) {
            baseTable = baseTable.substring(0, baseTable.length() - 1);
        }

        sql = new StringBuilder(baseTable);
        sql.append("\nPARTITION BY RANGE (UNIX_TIMESTAMP(")
           .append(escapeIdentifier(partitionColumn))
           .append("))");

        return sql.toString();
    }

    /**
     * Generate SQL for partition pruning optimization
     */
    public String generatePartitionPrunedQuery(String tableName, String dateColumn,
                                              String startDate, String endDate,
                                              List<String> partitions) {
        String partitionList = partitions.stream()
                .map(p -> escapeIdentifier(p))
                .collect(Collectors.joining(", "));

        return String.format("SELECT * FROM %s PARTITION (%s) WHERE %s >= ? AND %s < ?",
                escapeIdentifier(tableName),
                partitionList,
                escapeIdentifier(dateColumn),
                escapeIdentifier(dateColumn));
    }

    /**
     * Generate SQL for checking partition information
     */
    public String generateShowPartitions(String tableName) {
        return String.format(
            "SELECT PARTITION_NAME, PARTITION_EXPRESSION, PARTITION_DESCRIPTION " +
            "FROM INFORMATION_SCHEMA.PARTITIONS " +
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s' " +
            "AND PARTITION_NAME IS NOT NULL",
            tableName
        );
    }
}