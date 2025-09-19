package com.telcobright.core.persistence;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.metadata.EntityMetadata;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Database-agnostic persistence provider interface.
 * Provides abstraction for database-specific SQL operations and optimizations.
 *
 * @author Split-Verse Framework
 */
public interface PersistenceProvider {

    /**
     * Get the database type
     */
    DatabaseType getDatabaseType();

    /**
     * Generate CREATE TABLE SQL for the entity
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @param charset Character set for the table
     * @param collation Collation for the table
     * @return CREATE TABLE SQL statement
     */
    String generateCreateTableSQL(String tableName, EntityMetadata<?> metadata, String charset, String collation);

    /**
     * Generate INSERT SQL for a single entity
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return INSERT SQL statement with placeholders
     */
    String generateInsertSQL(String tableName, EntityMetadata<?> metadata);

    /**
     * Generate bulk INSERT SQL for multiple entities
     * Uses database-specific optimizations (e.g., MySQL extended INSERT syntax)
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @param entityCount Number of entities to insert
     * @return Bulk INSERT SQL statement with placeholders
     */
    String generateBulkInsertSQL(String tableName, EntityMetadata<?> metadata, int entityCount);

    /**
     * Generate UPDATE SQL for an entity by ID
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return UPDATE SQL statement with placeholders
     */
    String generateUpdateSQL(String tableName, EntityMetadata<?> metadata);

    /**
     * Generate DELETE SQL by ID
     * @param tableName Full table name including schema
     * @param idColumn Name of the ID column
     * @return DELETE SQL statement with placeholders
     */
    String generateDeleteSQL(String tableName, String idColumn);

    /**
     * Generate SELECT SQL for finding by ID
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return SELECT SQL statement with placeholders
     */
    String generateSelectByIdSQL(String tableName, EntityMetadata<?> metadata);

    /**
     * Generate SELECT SQL for finding by date range
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @param dateColumn Name of the date column
     * @return SELECT SQL statement with placeholders
     */
    String generateSelectByDateRangeSQL(String tableName, EntityMetadata<?> metadata, String dateColumn);

    /**
     * Generate SELECT SQL with pagination
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @param orderByColumn Column to order by
     * @param ascending Whether to sort ascending
     * @param limit Maximum number of records
     * @param offset Number of records to skip
     * @return SELECT SQL statement with pagination
     */
    String generateSelectWithPaginationSQL(String tableName, EntityMetadata<?> metadata,
                                          String orderByColumn, boolean ascending,
                                          int limit, int offset);

    /**
     * Execute bulk insert operation with optimal performance
     * @param connection Database connection
     * @param entities List of entities to insert
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return Number of records inserted
     */
    <T extends ShardingEntity> int executeBulkInsert(Connection connection, List<T> entities,
                                                     String tableName, EntityMetadata<T> metadata)
                                                     throws SQLException;

    /**
     * Execute single insert operation
     * @param connection Database connection
     * @param entity Entity to insert
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return Generated key if applicable, null otherwise
     */
    <T extends ShardingEntity> String executeInsert(Connection connection, T entity,
                                                    String tableName, EntityMetadata<T> metadata)
                                                    throws SQLException;

    /**
     * Execute update operation
     * @param connection Database connection
     * @param entity Entity to update
     * @param tableName Full table name including schema
     * @param metadata Entity metadata
     * @return Number of rows affected
     */
    <T extends ShardingEntity> int executeUpdate(Connection connection, T entity,
                                                 String tableName, EntityMetadata<T> metadata)
                                                 throws SQLException;

    /**
     * Execute delete operation
     * @param connection Database connection
     * @param id Entity ID to delete
     * @param tableName Full table name including schema
     * @param idColumn Name of the ID column
     * @return Number of rows affected
     */
    int executeDelete(Connection connection, String id, String tableName, String idColumn)
                     throws SQLException;

    /**
     * Map ResultSet to entity
     * @param rs ResultSet to map
     * @param metadata Entity metadata
     * @return Mapped entity
     */
    <T extends ShardingEntity> T mapResultSetToEntity(ResultSet rs, EntityMetadata<T> metadata)
                                                      throws SQLException;

    /**
     * Check if a table exists
     * @param connection Database connection
     * @param schema Database schema
     * @param tableName Table name (without schema prefix)
     * @return true if table exists
     */
    boolean tableExists(Connection connection, String schema, String tableName) throws SQLException;

    /**
     * Drop a table if it exists
     * @param connection Database connection
     * @param tableName Full table name including schema
     */
    void dropTableIfExists(Connection connection, String tableName) throws SQLException;

    /**
     * Get the appropriate date/time function for the database
     * @return Database-specific current timestamp function (e.g., NOW() for MySQL)
     */
    String getCurrentTimestampFunction();

    /**
     * Get the appropriate string concatenation operator
     * @return Database-specific concatenation operator (e.g., CONCAT for MySQL, || for PostgreSQL)
     */
    String getConcatenationOperator();

    /**
     * Get the appropriate limit/offset syntax
     * @param limit Maximum number of records
     * @param offset Number of records to skip
     * @return Database-specific LIMIT/OFFSET clause
     */
    String getLimitOffsetClause(int limit, int offset);

    /**
     * Validate database connection
     * @param connection Connection to validate
     * @return true if connection is valid
     */
    boolean isConnectionValid(Connection connection) throws SQLException;

    /**
     * Get database-specific connection URL
     * @param host Database host
     * @param port Database port
     * @param database Database name
     * @param additionalParams Additional connection parameters
     * @return JDBC connection URL
     */
    String getConnectionUrl(String host, int port, String database, Map<String, String> additionalParams);

    /**
     * Database types supported by the framework
     */
    enum DatabaseType {
        MYSQL("MySQL"),
        POSTGRESQL("PostgreSQL"),
        ORACLE("Oracle"),
        SQLSERVER("SQL Server"),
        MARIADB("MariaDB"),
        COCKROACHDB("CockroachDB"),
        TIDB("TiDB");

        private final String displayName;

        DatabaseType(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }
}