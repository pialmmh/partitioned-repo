package com.telcobright.core.sql.postgresql;

import com.telcobright.core.sql.BaseSqlGenerator;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PostgreSQL SQL Generator
 * Optimized SQL generation for PostgreSQL database with support for:
 * - Declarative partitioning (RANGE, LIST, HASH)
 * - PostgreSQL-specific features and optimizations
 * - Advanced indexing strategies
 */
public class PostgreSQLSqlGenerator extends BaseSqlGenerator {

    @Override
    public String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                                   String partitionColumn, String partitionType) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        if (supportsIfNotExists()) {
            sql.append("IF NOT EXISTS ");
        }
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

        sql.append(") PARTITION BY ");

        // PostgreSQL declarative partitioning
        if ("RANGE".equalsIgnoreCase(partitionType)) {
            sql.append("RANGE (");
            FieldMetadata partitionField = getFieldByColumnName(metadata, partitionColumn);
            if (partitionField != null && partitionField.getType() == java.time.LocalDateTime.class) {
                sql.append("DATE(").append(escapeIdentifier(partitionColumn)).append(")");
            } else {
                sql.append(escapeIdentifier(partitionColumn));
            }
            sql.append(")");
        } else if ("HASH".equalsIgnoreCase(partitionType)) {
            sql.append("HASH (").append(escapeIdentifier(partitionColumn)).append(")");
        } else {
            sql.append("LIST (").append(escapeIdentifier(partitionColumn)).append(")");
        }

        return sql.toString();
    }

    @Override
    public String generateAlterTableAddPartition(String tableName, String partitionName,
                                                String partitionDefinition) {
        // PostgreSQL uses CREATE TABLE for adding partitions
        return String.format("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)",
                escapeIdentifier(partitionName),
                escapeIdentifier(tableName),
                partitionDefinition.split(",")[0],
                partitionDefinition.split(",")[1]);
    }

    @Override
    public String generateCreateIndex(String tableName, String indexName, List<String> columns) {
        String columnList = columns.stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        // PostgreSQL supports various index types
        return String.format("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s USING btree (%s)",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }

    @Override
    public String generateBatchInsert(String tableName, List<Map<String, Object>> records) {
        // PostgreSQL supports COPY for efficient bulk insert, but we'll use multi-row INSERT
        String baseInsert = super.generateBatchInsert(tableName, records);

        // Add ON CONFLICT for upsert behavior
        Map<String, Object> firstRecord = records.get(0);
        String updateClause = firstRecord.keySet().stream()
                .filter(col -> !col.equals("id"))
                .map(col -> escapeIdentifier(col) + " = EXCLUDED." + escapeIdentifier(col))
                .collect(Collectors.joining(", "));

        return baseInsert + " ON CONFLICT (id) DO UPDATE SET " + updateClause;
    }

    @Override
    public String generateSelectWithPagination(String baseQuery, int limit, int offset) {
        // PostgreSQL supports standard LIMIT/OFFSET
        return baseQuery + " LIMIT " + limit + " OFFSET " + offset;
    }

    @Override
    public String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                                    String lastId, int batchSize) {
        // PostgreSQL with index-only scan optimization
        return String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(idColumn),
                batchSize);
    }

    @Override
    public String generateExistsQuery(String tableName, String idColumn) {
        // PostgreSQL-optimized EXISTS
        return String.format("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ?) AS exists",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String escapeIdentifier(String identifier) {
        // PostgreSQL uses double quotes for identifiers
        return "\"" + identifier + "\"";
    }

    @Override
    public String getCurrentTimestamp() {
        // PostgreSQL current timestamp
        return "CURRENT_TIMESTAMP";
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
            } else if (columnName.contains("email") || columnName.contains("name")) {
                return "VARCHAR(255)";
            } else {
                return "TEXT";
            }
        } else if (type == Integer.class || type == int.class) {
            return "INTEGER";
        } else if (type == Long.class || type == long.class) {
            return "BIGINT";
        } else if (type == java.time.LocalDateTime.class) {
            return "TIMESTAMP WITHOUT TIME ZONE";
        } else if (type == java.time.LocalDate.class) {
            return "DATE";
        } else if (type == java.time.LocalTime.class) {
            return "TIME WITHOUT TIME ZONE";
        } else if (type == Boolean.class || type == boolean.class) {
            return "BOOLEAN";
        } else if (type == Double.class || type == double.class) {
            return "DOUBLE PRECISION";
        } else if (type == Float.class || type == float.class) {
            return "REAL";
        } else if (type == java.math.BigDecimal.class) {
            return "NUMERIC(19, 4)";
        } else if (type == byte[].class) {
            return "BYTEA";
        } else if (type == java.util.UUID.class) {
            return "UUID";
        } else {
            return "TEXT";
        }
    }

    // PostgreSQL-specific methods

    /**
     * Generate SQL for creating a partitioned table with automatic partition management
     */
    public String generateAutoPartitionedTable(String tableName, EntityMetadata metadata,
                                              String partitionColumn, String interval) {
        StringBuilder sql = new StringBuilder(generateCreateTableWithPartitions(
            tableName, metadata, partitionColumn, "RANGE"));

        // Add comment for pg_partman extension if available
        sql.append(";\n-- For automatic partition management, use pg_partman extension");
        sql.append("\n-- SELECT partman.create_parent('public.").append(tableName)
           .append("', '").append(partitionColumn).append("', 'native', '")
           .append(interval).append("');");

        return sql.toString();
    }

    /**
     * Generate SQL for BRIN index (Block Range INdex) for time-series data
     */
    public String generateBrinIndex(String tableName, String indexName, String column) {
        return String.format("CREATE INDEX %s ON %s USING brin (%s)",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                escapeIdentifier(column));
    }

    /**
     * Generate SQL for checking partition information
     */
    public String generateShowPartitions(String tableName) {
        return String.format(
            "SELECT " +
            "    nmsp_parent.nspname AS parent_schema, " +
            "    parent.relname AS parent_table, " +
            "    nmsp_child.nspname AS child_schema, " +
            "    child.relname AS child_table " +
            "FROM pg_inherits " +
            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
            "JOIN pg_namespace nmsp_parent ON parent.relnamespace = nmsp_parent.oid " +
            "JOIN pg_namespace nmsp_child ON child.relnamespace = nmsp_child.oid " +
            "WHERE parent.relname = '%s'",
            tableName
        );
    }

    /**
     * Generate SQL for parallel query hint
     */
    public String generateParallelQuery(String baseQuery, int parallelWorkers) {
        return String.format("/*+ parallel(%d) */ %s", parallelWorkers, baseQuery);
    }
}