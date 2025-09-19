package com.telcobright.core.persistence;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import com.telcobright.core.sql.SqlStatementCache;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * MySQL-specific persistence provider implementation.
 * Optimized for MySQL with features like extended INSERT syntax for bulk operations.
 *
 * @author Split-Verse Framework
 */
public class MySQLPersistenceProvider implements PersistenceProvider {

    private static final String JDBC_PREFIX = "jdbc:mysql://";
    private static final int DEFAULT_PORT = 3306;
    private final SqlStatementCache sqlCache;

    public MySQLPersistenceProvider() {
        this.sqlCache = null;
    }

    public MySQLPersistenceProvider(SqlStatementCache cache) {
        this.sqlCache = cache;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.MYSQL;
    }

    @Override
    public String generateCreateTableSQL(String tableName, EntityMetadata<?> metadata,
                                        String charset, String collation) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        // Add columns
        metadata.getFields().forEach(field -> {
            sql.append("\n  ").append(field.getColumnName()).append(" ");

            // Map Java types to MySQL types
            Class<?> type = field.getType();
            if (type == String.class) {
                if (field.isId()) {
                    sql.append("VARCHAR(255)");
                } else {
                    sql.append("TEXT");
                }
            } else if (type == Long.class || type == long.class) {
                sql.append("BIGINT");
            } else if (type == Integer.class || type == int.class) {
                sql.append("INT");
            } else if (type == Double.class || type == double.class) {
                sql.append("DOUBLE");
            } else if (type == Float.class || type == float.class) {
                sql.append("FLOAT");
            } else if (type == Boolean.class || type == boolean.class) {
                sql.append("BOOLEAN");
            } else if (type == LocalDateTime.class) {
                sql.append("DATETIME(6)");
            } else if (type == java.time.LocalDate.class) {
                sql.append("DATE");
            } else if (type == java.time.LocalTime.class) {
                sql.append("TIME");
            } else if (type == java.math.BigDecimal.class) {
                sql.append("DECIMAL(19,4)");
            } else {
                sql.append("TEXT");
            }

            // Add constraints
            if (field.isId()) {
                sql.append(" PRIMARY KEY");
            } else if (!field.isNullable()) {
                sql.append(" NOT NULL");
            }

            sql.append(",");
        });

        // Add indexes for sharding key
        if (metadata.getShardingKeyField() != null) {
            sql.append("\n  INDEX idx_").append(metadata.getShardingKeyField().getColumnName())
               .append(" (").append(metadata.getShardingKeyField().getColumnName()).append("),");
        }

        // Remove trailing comma
        if (sql.charAt(sql.length() - 1) == ',') {
            sql.setLength(sql.length() - 1);
        }

        sql.append("\n) ENGINE=InnoDB");

        // Add charset and collation
        if (charset != null && !charset.isEmpty()) {
            sql.append(" DEFAULT CHARSET=").append(charset);
        }
        if (collation != null && !collation.isEmpty()) {
            sql.append(" COLLATE=").append(collation);
        }

        return sql.toString();
    }

    @Override
    public String generateInsertSQL(String tableName, EntityMetadata<?> metadata) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder values = new StringBuilder(" VALUES (");

        metadata.getFields().forEach(field -> {
            sql.append(field.getColumnName()).append(", ");
            values.append("?, ");
        });

        // Remove trailing commas
        sql.setLength(sql.length() - 2);
        values.setLength(values.length() - 2);

        sql.append(")").append(values).append(")");
        return sql.toString();
    }

    @Override
    public String generateBulkInsertSQL(String tableName, EntityMetadata<?> metadata, int entityCount) {
        if (entityCount <= 0) {
            throw new IllegalArgumentException("Entity count must be positive");
        }

        // Check cache first if available
        if (sqlCache != null) {
            SqlStatementCache.StatementType cacheType = getCacheTypeForBatchSize(entityCount);
            if (cacheType != null) {
                Class<?> entityClass = metadata.getClass();
                // Extract entity class from metadata - need to get the actual entity class, not metadata class
                // This is a workaround - in production, we'd pass the entity class explicitly
                String cachedSql = sqlCache.get(entityClass, tableName, cacheType);
                if (cachedSql != null) {
                    return cachedSql;
                }
            }
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");

        // Add column names
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));
        sql.append(columns).append(") VALUES ");

        // Add value placeholders for each entity
        // MySQL extended INSERT syntax: INSERT INTO table (cols) VALUES (?,?), (?,?), (?,?)
        StringJoiner valueGroups = new StringJoiner(", ");
        String singleValueGroup = "(" +
            metadata.getFields().stream()
                .map(f -> "?")
                .collect(Collectors.joining(", ")) + ")";

        for (int i = 0; i < entityCount; i++) {
            valueGroups.add(singleValueGroup);
        }

        sql.append(valueGroups.toString());
        String sqlString = sql.toString();

        // Store in cache if available and if it's a standard batch size
        if (sqlCache != null) {
            SqlStatementCache.StatementType cacheType = getCacheTypeForBatchSize(entityCount);
            if (cacheType != null) {
                // We need the actual entity class - this is a limitation we'd fix in production
                // For now, we won't cache here as we don't have the entity class
                // The SqlGeneratorByEntityRegistry will handle pre-generation instead
            }
        }

        return sqlString;
    }

    private SqlStatementCache.StatementType getCacheTypeForBatchSize(int size) {
        if (size == 1) return SqlStatementCache.StatementType.INSERT_SINGLE;
        if (size == 10) return SqlStatementCache.StatementType.INSERT_BATCH_10;
        if (size == 100) return SqlStatementCache.StatementType.INSERT_BATCH_100;
        if (size == 1000) return SqlStatementCache.StatementType.INSERT_BATCH_1000;
        if (size == 5000) return SqlStatementCache.StatementType.INSERT_BATCH_5000;
        return null; // Don't cache other sizes
    }

    @Override
    public String generateUpdateSQL(String tableName, EntityMetadata<?> metadata) {
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");

        metadata.getFields().stream()
            .filter(field -> !field.isId())
            .forEach(field -> sql.append(field.getColumnName()).append(" = ?, "));

        // Remove trailing comma
        sql.setLength(sql.length() - 2);

        // Add WHERE clause for ID
        sql.append(" WHERE ").append(metadata.getIdField().getColumnName()).append(" = ?");

        return sql.toString();
    }

    @Override
    public String generateDeleteSQL(String tableName, String idColumn) {
        return "DELETE FROM " + tableName + " WHERE " + idColumn + " = ?";
    }

    @Override
    public String generateSelectByIdSQL(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        return "SELECT " + columns + " FROM " + tableName +
               " WHERE " + metadata.getIdField().getColumnName() + " = ?";
    }

    @Override
    public String generateSelectByDateRangeSQL(String tableName, EntityMetadata<?> metadata,
                                              String dateColumn) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        return "SELECT " + columns + " FROM " + tableName +
               " WHERE " + dateColumn + " >= ? AND " + dateColumn + " <= ?" +
               " ORDER BY " + dateColumn;
    }

    @Override
    public String generateSelectWithPaginationSQL(String tableName, EntityMetadata<?> metadata,
                                                 String orderByColumn, boolean ascending,
                                                 int limit, int offset) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String orderDirection = ascending ? "ASC" : "DESC";

        return "SELECT " + columns + " FROM " + tableName +
               " ORDER BY " + orderByColumn + " " + orderDirection +
               " LIMIT " + limit + " OFFSET " + offset;
    }

    @Override
    public <T extends ShardingEntity> int executeBulkInsert(Connection connection, List<T> entities,
                                                           String tableName, EntityMetadata<T> metadata)
                                                           throws SQLException {
        if (entities == null || entities.isEmpty()) {
            return 0;
        }

        // Use MySQL extended INSERT syntax for optimal performance
        // INSERT INTO table (cols) VALUES (row1), (row2), (row3)...
        String sql = generateBulkInsertSQL(tableName, metadata, entities.size());

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int paramIndex = 1;

            // Set parameters for all entities
            for (T entity : entities) {
                for (FieldMetadata field : metadata.getFields()) {
                    Object value = field.getValue(entity);
                    setParameter(stmt, paramIndex++, value);
                }
            }

            return stmt.executeUpdate();
        }
    }

    @Override
    public <T extends ShardingEntity> String executeInsert(Connection connection, T entity,
                                                          String tableName, EntityMetadata<T> metadata)
                                                          throws SQLException {
        String sql = generateInsertSQL(tableName, metadata);

        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            // Set parameters
            metadata.setInsertParameters(stmt, entity);

            int affectedRows = stmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Creating entity failed, no rows affected.");
            }

            // Return the ID (which should already be set on the entity for String IDs)
            return entity.getId();
        }
    }

    @Override
    public <T extends ShardingEntity> int executeUpdate(Connection connection, T entity,
                                                       String tableName, EntityMetadata<T> metadata)
                                                       throws SQLException {
        String sql = generateUpdateSQL(tableName, metadata);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int paramIndex = 1;

            // Set non-ID field values
            for (FieldMetadata field : metadata.getFields()) {
                if (!field.isId()) {
                    Object value = field.getValue(entity);
                    setParameter(stmt, paramIndex++, value);
                }
            }

            // Set ID for WHERE clause
            stmt.setString(paramIndex, entity.getId());

            return stmt.executeUpdate();
        }
    }

    @Override
    public int executeDelete(Connection connection, String id, String tableName, String idColumn)
                            throws SQLException {
        String sql = generateDeleteSQL(tableName, idColumn);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            return stmt.executeUpdate();
        }
    }

    @Override
    public <T extends ShardingEntity> T mapResultSetToEntity(ResultSet rs, EntityMetadata<T> metadata)
                                                            throws SQLException {
        return metadata.mapResultSet(rs);
    }

    @Override
    public boolean tableExists(Connection connection, String schema, String tableName) throws SQLException {
        String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    @Override
    public void dropTableIfExists(Connection connection, String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "NOW(6)"; // MySQL with microsecond precision
    }

    @Override
    public String getConcatenationOperator() {
        return "CONCAT";
    }

    @Override
    public String getLimitOffsetClause(int limit, int offset) {
        if (offset > 0) {
            return " LIMIT " + limit + " OFFSET " + offset;
        }
        return " LIMIT " + limit;
    }

    @Override
    public boolean isConnectionValid(Connection connection) throws SQLException {
        return connection != null && !connection.isClosed() && connection.isValid(1);
    }

    @Override
    public String getConnectionUrl(String host, int port, String database, Map<String, String> additionalParams) {
        StringBuilder url = new StringBuilder(JDBC_PREFIX);
        url.append(host).append(":").append(port > 0 ? port : DEFAULT_PORT);
        url.append("/").append(database);

        // Add default parameters
        url.append("?useSSL=false");
        url.append("&serverTimezone=UTC");
        url.append("&useLegacyDatetimeCode=false");
        url.append("&preserveInstants=true");
        url.append("&rewriteBatchedStatements=true"); // Important for bulk insert performance
        url.append("&useServerPrepStmts=true");
        url.append("&cachePrepStmts=true");

        // Add additional parameters
        if (additionalParams != null && !additionalParams.isEmpty()) {
            additionalParams.forEach((key, value) ->
                url.append("&").append(key).append("=").append(value)
            );
        }

        return url.toString();
    }

    /**
     * Helper method to set parameter values with proper type handling
     */
    private void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            stmt.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof LocalDateTime) {
            stmt.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof java.time.LocalDate) {
            stmt.setDate(index, Date.valueOf((java.time.LocalDate) value));
        } else if (value instanceof java.time.LocalTime) {
            stmt.setTime(index, Time.valueOf((java.time.LocalTime) value));
        } else if (value instanceof java.math.BigDecimal) {
            stmt.setBigDecimal(index, (java.math.BigDecimal) value);
        } else {
            stmt.setObject(index, value);
        }
    }
}