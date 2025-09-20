package com.telcobright.core.sql;

import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base SQL Generator - ANSI SQL Implementation
 * Provides default implementations using standard ANSI SQL syntax
 */
public class BaseSqlGenerator implements SqlGenerator {

    protected static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String generateCreateTable(String tableName, EntityMetadata metadata) {
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

        sql.append(")");
        return sql.toString();
    }

    @Override
    public String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                                   String partitionColumn, String partitionType) {
        // Default implementation - no native partitioning support in ANSI SQL
        return generateCreateTable(tableName, metadata);
    }

    @Override
    public String generateAlterTableAddPartition(String tableName, String partitionName,
                                                String partitionDefinition) {
        throw new UnsupportedOperationException("Native partitioning not supported in ANSI SQL");
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
        return String.format("CREATE INDEX %s ON %s (%s)",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }

    @Override
    public String generateDropIndex(String tableName, String indexName) {
        return "DROP INDEX " + escapeIdentifier(indexName);
    }

    @Override
    public String generateInsert(String tableName, Map<String, Object> values) {
        String columns = values.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = values.values().stream()
                .map(v -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                escapeIdentifier(tableName), columns, placeholders);
    }

    @Override
    public String generateBatchInsert(String tableName, List<Map<String, Object>> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate batch insert for empty records");
        }

        Map<String, Object> firstRecord = records.get(0);
        String columns = firstRecord.keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String valuePlaceholders = firstRecord.keySet().stream()
                .map(k -> "?")
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(escapeIdentifier(tableName))
           .append(" (").append(columns).append(") VALUES ");

        for (int i = 0; i < records.size(); i++) {
            sql.append("(").append(valuePlaceholders).append(")");
            if (i < records.size() - 1) {
                sql.append(", ");
            }
        }

        return sql.toString();
    }

    @Override
    public String generateSelectById(String tableName, String idColumn) {
        return String.format("SELECT * FROM %s WHERE %s = ?",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String generateSelectAll(String tableName) {
        return "SELECT * FROM " + escapeIdentifier(tableName);
    }

    @Override
    public String generateSelectWithWhere(String tableName, Map<String, Object> conditions) {
        if (conditions.isEmpty()) {
            return generateSelectAll(tableName);
        }

        String whereClause = conditions.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("SELECT * FROM %s WHERE %s",
                escapeIdentifier(tableName), whereClause);
    }

    @Override
    public String generateSelectWithDateRange(String tableName, String dateColumn,
                                             String startDate, String endDate) {
        return String.format("SELECT * FROM %s WHERE %s >= ? AND %s < ?",
                escapeIdentifier(tableName),
                escapeIdentifier(dateColumn),
                escapeIdentifier(dateColumn));
    }

    @Override
    public String generateSelectByIdAndDateRange(String tableName, String idColumn,
                                                String dateColumn, String startDate, String endDate) {
        return String.format("SELECT * FROM %s WHERE %s = ? AND %s >= ? AND %s < ?",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(dateColumn),
                escapeIdentifier(dateColumn));
    }

    @Override
    public String generateUpdate(String tableName, Map<String, Object> values,
                                Map<String, Object> conditions) {
        String setClause = values.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(", "));

        String whereClause = conditions.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("UPDATE %s SET %s WHERE %s",
                escapeIdentifier(tableName), setClause, whereClause);
    }

    @Override
    public String generateUpdateById(String tableName, String idColumn, Map<String, Object> values) {
        String setClause = values.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(", "));

        return String.format("UPDATE %s SET %s WHERE %s = ?",
                escapeIdentifier(tableName), setClause, escapeIdentifier(idColumn));
    }

    @Override
    public String generateDelete(String tableName, Map<String, Object> conditions) {
        String whereClause = conditions.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("DELETE FROM %s WHERE %s",
                escapeIdentifier(tableName), whereClause);
    }

    @Override
    public String generateDeleteById(String tableName, String idColumn) {
        return String.format("DELETE FROM %s WHERE %s = ?",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String generateDeleteByDateRange(String tableName, String dateColumn,
                                           String startDate, String endDate) {
        return String.format("DELETE FROM %s WHERE %s >= ? AND %s < ?",
                escapeIdentifier(tableName),
                escapeIdentifier(dateColumn),
                escapeIdentifier(dateColumn));
    }

    @Override
    public String generateSelectWithPagination(String baseQuery, int limit, int offset) {
        return baseQuery + " LIMIT " + limit + " OFFSET " + offset;
    }

    @Override
    public String generateSelectWithSorting(String baseQuery, List<SortField> sortFields) {
        if (sortFields.isEmpty()) {
            return baseQuery;
        }

        String orderByClause = sortFields.stream()
                .map(sf -> escapeIdentifier(sf.getColumn()) + " " + sf.getDirection())
                .collect(Collectors.joining(", "));

        return baseQuery + " ORDER BY " + orderByClause;
    }

    @Override
    public String generateCountQuery(String tableName, Map<String, Object> conditions) {
        if (conditions.isEmpty()) {
            return "SELECT COUNT(*) FROM " + escapeIdentifier(tableName);
        }

        String whereClause = conditions.keySet().stream()
                .map(col -> escapeIdentifier(col) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("SELECT COUNT(*) FROM %s WHERE %s",
                escapeIdentifier(tableName), whereClause);
    }

    @Override
    public String generateSelectBatchByIdGreaterThan(String tableName, String idColumn,
                                                    String lastId, int batchSize) {
        return String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn),
                escapeIdentifier(idColumn),
                batchSize);
    }

    @Override
    public String generateSelectBeforeDate(String tableName, String dateColumn, String date) {
        return String.format("SELECT * FROM %s WHERE %s < ?",
                escapeIdentifier(tableName),
                escapeIdentifier(dateColumn));
    }

    @Override
    public String generateSelectAfterDate(String tableName, String dateColumn, String date) {
        return String.format("SELECT * FROM %s WHERE %s >= ?",
                escapeIdentifier(tableName),
                escapeIdentifier(dateColumn));
    }

    @Override
    public String generateExistsQuery(String tableName, String idColumn) {
        return String.format("SELECT 1 FROM %s WHERE %s = ? LIMIT 1",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String escapeIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String escapeLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof LocalDateTime) {
            return "'" + DATE_TIME_FORMATTER.format((LocalDateTime) value) + "'";
        }
        return value.toString();
    }

    @Override
    public String getDateFormat() {
        return "yyyy-MM-dd HH:mm:ss";
    }

    @Override
    public String getCurrentTimestamp() {
        return "CURRENT_TIMESTAMP";
    }

    @Override
    public boolean supportsIfNotExists() {
        return false;
    }

    @Override
    public boolean supportsNativePartitioning() {
        return false;
    }

    protected FieldMetadata getFieldByColumnName(EntityMetadata<?> metadata, String columnName) {
        for (FieldMetadata field : metadata.getFields()) {
            if (field.getColumnName().equals(columnName)) {
                return field;
            }
        }
        return null;
    }

    protected String getSqlType(FieldMetadata field) {
        Class<?> type = field.getType();
        if (type == String.class) {
            return field.getColumnName().equals("id") ? "VARCHAR(255)" : "TEXT";
        } else if (type == Integer.class || type == int.class) {
            return "INTEGER";
        } else if (type == Long.class || type == long.class) {
            return "BIGINT";
        } else if (type == LocalDateTime.class) {
            return "TIMESTAMP";
        } else if (type == Boolean.class || type == boolean.class) {
            return "BOOLEAN";
        } else if (type == Double.class || type == double.class) {
            return "DOUBLE PRECISION";
        } else if (type == Float.class || type == float.class) {
            return "REAL";
        } else {
            return "TEXT";
        }
    }
}