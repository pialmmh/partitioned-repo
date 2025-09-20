package com.telcobright.core.sql.sqlserver;

import com.telcobright.core.sql.BaseSqlGenerator;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL Server SQL Generator
 * Optimized SQL generation for Microsoft SQL Server with support for:
 * - Table and index partitioning
 * - SQL Server-specific features and optimizations
 * - Temporal tables and columnstore indexes
 */
public class SqlServerSqlGenerator extends BaseSqlGenerator {

    @Override
    public String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                                   String partitionColumn, String partitionType) {
        // SQL Server requires partition function and scheme first
        StringBuilder sql = new StringBuilder();

        // Create partition function
        String functionName = "PF_" + tableName;
        String schemeName = "PS_" + tableName;

        sql.append("-- Create partition function\n");
        sql.append("CREATE PARTITION FUNCTION ").append(escapeIdentifier(functionName));

        FieldMetadata partitionField = getFieldByColumnName(metadata, partitionColumn);
        if (partitionField != null && partitionField.getType() == java.time.LocalDateTime.class) {
            sql.append("(datetime2)\n");
            sql.append("AS RANGE RIGHT FOR VALUES ();\n\n");
        } else {
            sql.append("(").append(getSqlType(partitionField)).append(")\n");
            sql.append("AS RANGE RIGHT FOR VALUES ();\n\n");
        }

        // Create partition scheme
        sql.append("-- Create partition scheme\n");
        sql.append("CREATE PARTITION SCHEME ").append(escapeIdentifier(schemeName)).append("\n");
        sql.append("AS PARTITION ").append(escapeIdentifier(functionName)).append("\n");
        sql.append("ALL TO ([PRIMARY]);\n\n");

        // Create the table
        sql.append("-- Create partitioned table\n");
        sql.append("CREATE TABLE ").append(escapeIdentifier(tableName)).append(" (\n");

        // Add columns
        List<FieldMetadata> fields = metadata.getFields();
        for (int i = 0; i < fields.size(); i++) {
            FieldMetadata field = fields.get(i);
            sql.append("  ").append(escapeIdentifier(field.getColumnName()))
               .append(" ").append(getSqlType(field));

            if (field.equals(metadata.getIdField())) {
                // SQL Server clustered index for primary key
                sql.append(" NOT NULL");
            }

            if (i < fields.size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        // Add primary key constraint
        sql.append(",  CONSTRAINT PK_").append(tableName)
           .append(" PRIMARY KEY CLUSTERED (")
           .append(escapeIdentifier(metadata.getIdField().getColumnName()));

        // Include partition column in primary key for partitioning
        if (!metadata.getIdField().getColumnName().equals(partitionColumn)) {
            sql.append(", ").append(escapeIdentifier(partitionColumn));
        }
        sql.append(")\n");

        sql.append(") ON ").append(escapeIdentifier(schemeName))
           .append("(").append(escapeIdentifier(partitionColumn)).append(")");

        return sql.toString();
    }

    @Override
    public String generateAlterTableAddPartition(String tableName, String partitionName,
                                                String partitionDefinition) {
        // SQL Server adds partitions by altering the partition function
        String functionName = "PF_" + tableName;
        return String.format("ALTER PARTITION FUNCTION %s() SPLIT RANGE (%s)",
                escapeIdentifier(functionName), partitionDefinition);
    }

    @Override
    public String generateDropTable(String tableName) {
        return "DROP TABLE IF EXISTS " + escapeIdentifier(tableName);
    }

    @Override
    public String generateCreateIndex(String tableName, String indexName, List<String> columns) {
        String columnList = columns.stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        // SQL Server nonclustered index
        return String.format("CREATE NONCLUSTERED INDEX %s ON %s (%s)",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }

    @Override
    public String generateDropIndex(String tableName, String indexName) {
        // SQL Server requires table name in DROP INDEX
        return String.format("DROP INDEX %s ON %s",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName));
    }

    @Override
    public String generateBatchInsert(String tableName, List<Map<String, Object>> records) {
        // SQL Server supports table value constructors
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

        return sql.toString();
    }

    @Override
    public String generateSelectWithPagination(String baseQuery, int limit, int offset) {
        // SQL Server 2012+ supports OFFSET/FETCH
        // Requires ORDER BY clause
        if (!baseQuery.toUpperCase().contains("ORDER BY")) {
            baseQuery += " ORDER BY 1"; // Default ordering
        }
        return baseQuery + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }

    @Override
    public String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                                    String lastId, int batchSize) {
        // SQL Server with TOP clause
        return String.format("SELECT TOP %d * FROM %s WHERE %s > ? ORDER BY %s",
                batchSize,
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(idColumn));
    }

    @Override
    public String generateExistsQuery(String tableName, String idColumn) {
        // SQL Server EXISTS optimization
        return String.format(
            "SELECT CASE WHEN EXISTS(SELECT 1 FROM %s WHERE %s = ?) THEN 1 ELSE 0 END AS record_exists",
            escapeIdentifier(tableName),
            escapeIdentifier(idColumn)
        );
    }

    @Override
    public String escapeIdentifier(String identifier) {
        // SQL Server uses square brackets for identifiers
        return "[" + identifier + "]";
    }

    @Override
    public String getCurrentTimestamp() {
        // SQL Server current timestamp
        return "GETDATE()";
    }

    @Override
    public boolean supportsIfNotExists() {
        // SQL Server 2016+ supports DROP TABLE IF EXISTS
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
                return "NVARCHAR(255)";
            } else if (columnName.contains("email") || columnName.contains("name")) {
                return "NVARCHAR(255)";
            } else {
                return "NVARCHAR(MAX)";
            }
        } else if (type == Integer.class || type == int.class) {
            return "INT";
        } else if (type == Long.class || type == long.class) {
            return "BIGINT";
        } else if (type == java.time.LocalDateTime.class) {
            return "DATETIME2";
        } else if (type == java.time.LocalDate.class) {
            return "DATE";
        } else if (type == java.time.LocalTime.class) {
            return "TIME";
        } else if (type == Boolean.class || type == boolean.class) {
            return "BIT";
        } else if (type == Double.class || type == double.class) {
            return "FLOAT";
        } else if (type == Float.class || type == float.class) {
            return "REAL";
        } else if (type == java.math.BigDecimal.class) {
            return "DECIMAL(19, 4)";
        } else if (type == byte[].class) {
            return "VARBINARY(MAX)";
        } else if (type == java.util.UUID.class) {
            return "UNIQUEIDENTIFIER";
        } else {
            return "NVARCHAR(MAX)";
        }
    }

    // SQL Server-specific methods

    /**
     * Generate SQL for MERGE statement (upsert)
     */
    public String generateMerge(String tableName, Map<String, Object> values, String idColumn) {
        String columns = values.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String updateSetClause = values.keySet().stream()
                .filter(col -> !col.equals(idColumn))
                .map(col -> "T." + escapeIdentifier(col) + " = S." + escapeIdentifier(col))
                .collect(Collectors.joining(", "));

        String valuePlaceholders = values.keySet().stream()
                .map(k -> "? AS " + escapeIdentifier(k))
                .collect(Collectors.joining(", "));

        return String.format(
            "MERGE %s AS T " +
            "USING (SELECT %s) AS S " +
            "ON T.%s = S.%s " +
            "WHEN MATCHED THEN UPDATE SET %s " +
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
            escapeIdentifier(tableName),
            valuePlaceholders,
            escapeIdentifier(idColumn), escapeIdentifier(idColumn),
            updateSetClause,
            columns, columns.replace("[", "S.[")
        );
    }

    /**
     * Generate SQL for temporal table (system-versioned table)
     */
    public String generateTemporalTable(String tableName, EntityMetadata metadata) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(escapeIdentifier(tableName)).append(" (\n");

        // Add columns
        List<FieldMetadata> fields = metadata.getFields();
        for (FieldMetadata field : fields) {
            sql.append("  ").append(escapeIdentifier(field.getColumnName()))
               .append(" ").append(getSqlType(field));

            if (field.equals(metadata.getIdField())) {
                sql.append(" NOT NULL PRIMARY KEY");
            }
            sql.append(",\n");
        }

        // Add temporal columns
        sql.append("  SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,\n");
        sql.append("  SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,\n");
        sql.append("  PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)\n");
        sql.append(") WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.")
           .append(tableName).append("_History))");

        return sql.toString();
    }

    /**
     * Generate SQL for checking partition information
     */
    public String generateShowPartitions(String tableName) {
        return String.format(
            "SELECT " +
            "  p.partition_number, " +
            "  p.rows, " +
            "  prv.value AS boundary_value " +
            "FROM sys.partitions p " +
            "JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id " +
            "LEFT JOIN sys.partition_range_values prv ON prv.function_id = i.data_space_id " +
            "WHERE OBJECT_NAME(p.object_id) = '%s' " +
            "ORDER BY p.partition_number",
            tableName
        );
    }

    /**
     * Generate SQL for columnstore index
     */
    public String generateColumnstoreIndex(String tableName, String indexName) {
        return String.format("CREATE CLUSTERED COLUMNSTORE INDEX %s ON %s",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName));
    }
}