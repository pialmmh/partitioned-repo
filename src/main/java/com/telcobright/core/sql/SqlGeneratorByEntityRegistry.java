package com.telcobright.core.sql;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import com.telcobright.core.persistence.PersistenceProvider;
import com.telcobright.core.logging.Logger;
import com.telcobright.core.logging.ConsoleLogger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registry for pre-generating and caching SQL statements for entities.
 * Generates SQL for common batch sizes at startup to avoid runtime overhead.
 *
 * @author Split-Verse Framework
 */
public class SqlGeneratorByEntityRegistry {

    private final Map<Class<?>, EntityMetadata<?>> entityMetadataMap = new ConcurrentHashMap<>();
    private final Map<PersistenceProvider.DatabaseType, SqlStatementCache> cacheByDatabase = new ConcurrentHashMap<>();
    private final Logger logger;
    private SqlStatementCache cache;

    // Common batch sizes to pre-generate
    private static final int[] BATCH_SIZES = {10, 100, 1000, 5000};

    // Singleton instance
    private static SqlGeneratorByEntityRegistry instance;

    public SqlGeneratorByEntityRegistry() {
        this.logger = new ConsoleLogger("SqlGeneratorRegistry");
        this.cache = new SqlStatementCache();
    }

    public SqlGeneratorByEntityRegistry(SqlStatementCache cache) {
        this.logger = new ConsoleLogger("SqlGeneratorRegistry");
        this.cache = cache;
    }

    public SqlStatementCache getCache() {
        return cache;
    }

    public void setCache(SqlStatementCache cache) {
        this.cache = cache;
    }

    /**
     * Get singleton instance
     */
    public static synchronized SqlGeneratorByEntityRegistry getInstance() {
        if (instance == null) {
            instance = new SqlGeneratorByEntityRegistry();
        }
        return instance;
    }

    /**
     * Register an entity class for SQL pre-generation
     */
    public <T extends ShardingEntity> void registerEntity(Class<T> entityClass) {
        if (entityMetadataMap.containsKey(entityClass)) {
            logger.debug("Entity already registered: " + entityClass.getSimpleName());
            return;
        }

        EntityMetadata<T> metadata = new EntityMetadata<>(entityClass);
        entityMetadataMap.put(entityClass, metadata);

        logger.info("Registered entity for SQL generation: " + entityClass.getSimpleName());
    }

    /**
     * Pre-generate SQL for all registered entities and specified tables
     */
    public void pregenerateForTables(PersistenceProvider.DatabaseType dbType,
                                    Map<Class<?>, List<String>> entityTableMap) {
        // Use provided cache or create one for the database type
        if (cache == null) {
            cache = cacheByDatabase.computeIfAbsent(dbType, k -> new SqlStatementCache());
        }

        for (Map.Entry<Class<?>, List<String>> entry : entityTableMap.entrySet()) {
            Class<?> entityClass = entry.getKey();
            List<String> tableNames = entry.getValue();

            EntityMetadata<?> metadata = entityMetadataMap.get(entityClass);
            if (metadata == null) {
                registerEntity((Class<? extends ShardingEntity>) entityClass);
                metadata = entityMetadataMap.get(entityClass);
            }

            for (String tableName : tableNames) {
                pregenerateForTable(dbType, entityClass, tableName, metadata, cache);
            }
        }

        SqlStatementCache.CacheStatistics stats = cache.getStatistics();
        logger.info(String.format("Pre-generated SQL for %s: %s", dbType, stats));
    }

    /**
     * Pre-generate SQL for a specific entity and table
     */
    private void pregenerateForTable(PersistenceProvider.DatabaseType dbType,
                                    Class<?> entityClass,
                                    String tableName,
                                    EntityMetadata<?> metadata,
                                    SqlStatementCache cache) {

        switch (dbType) {
            case MYSQL:
            case MARIADB:
            case TIDB:
                pregenerateMySQLStatements(entityClass, tableName, metadata, cache);
                break;
            case POSTGRESQL:
            case COCKROACHDB:
                pregeneratePostgreSQLStatements(entityClass, tableName, metadata, cache);
                break;
            case ORACLE:
                pregenerateOracleStatements(entityClass, tableName, metadata, cache);
                break;
            case SQLSERVER:
                pregenerateSQLServerStatements(entityClass, tableName, metadata, cache);
                break;
            default:
                logger.warn("Unsupported database type for pre-generation: " + dbType);
        }
    }

    /**
     * Generate MySQL statements
     */
    private void pregenerateMySQLStatements(Class<?> entityClass, String tableName,
                                           EntityMetadata<?> metadata, SqlStatementCache cache) {
        // Single INSERT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_SINGLE,
                 generateMySQLInsert(tableName, metadata));

        // Batch INSERTs for different sizes
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_10,
                 generateMySQLBatchInsert(tableName, metadata, 10));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_100,
                 generateMySQLBatchInsert(tableName, metadata, 100));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_1000,
                 generateMySQLBatchInsert(tableName, metadata, 1000));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_5000,
                 generateMySQLBatchInsert(tableName, metadata, 5000));

        // UPDATE BY ID
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.UPDATE_BY_ID,
                 generateMySQLUpdate(tableName, metadata));

        // DELETE BY ID
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.DELETE_BY_ID,
                 generateMySQLDelete(tableName, metadata));

        // SELECT statements
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.SELECT_BY_ID,
                 generateMySQLSelectById(tableName, metadata));
    }

    /**
     * Generate PostgreSQL statements
     */
    private void pregeneratePostgreSQLStatements(Class<?> entityClass, String tableName,
                                                EntityMetadata<?> metadata, SqlStatementCache cache) {
        // Single INSERT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_SINGLE,
                 generatePostgreSQLInsert(tableName, metadata));

        // Batch INSERTs
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_10,
                 generatePostgreSQLBatchInsert(tableName, metadata, 10));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_100,
                 generatePostgreSQLBatchInsert(tableName, metadata, 100));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_1000,
                 generatePostgreSQLBatchInsert(tableName, metadata, 1000));

        // UPDATE, DELETE, SELECT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.UPDATE_BY_ID,
                 generatePostgreSQLUpdate(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.DELETE_BY_ID,
                 generatePostgreSQLDelete(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.SELECT_BY_ID,
                 generatePostgreSQLSelectById(tableName, metadata));
    }

    /**
     * Generate Oracle statements
     */
    private void pregenerateOracleStatements(Class<?> entityClass, String tableName,
                                            EntityMetadata<?> metadata, SqlStatementCache cache) {
        // Single INSERT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_SINGLE,
                 generateOracleInsert(tableName, metadata));

        // Oracle batch inserts use INSERT ALL
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_10,
                 generateOracleBatchInsert(tableName, metadata, 10));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_100,
                 generateOracleBatchInsert(tableName, metadata, 100));

        // UPDATE, DELETE, SELECT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.UPDATE_BY_ID,
                 generateOracleUpdate(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.DELETE_BY_ID,
                 generateOracleDelete(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.SELECT_BY_ID,
                 generateOracleSelectById(tableName, metadata));
    }

    /**
     * Generate SQL Server statements
     */
    private void pregenerateSQLServerStatements(Class<?> entityClass, String tableName,
                                               EntityMetadata<?> metadata, SqlStatementCache cache) {
        // Single INSERT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_SINGLE,
                 generateSQLServerInsert(tableName, metadata));

        // Batch INSERTs (similar to MySQL)
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_10,
                 generateSQLServerBatchInsert(tableName, metadata, 10));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_100,
                 generateSQLServerBatchInsert(tableName, metadata, 100));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.INSERT_BATCH_1000,
                 generateSQLServerBatchInsert(tableName, metadata, 1000));

        // UPDATE, DELETE, SELECT
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.UPDATE_BY_ID,
                 generateSQLServerUpdate(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.DELETE_BY_ID,
                 generateSQLServerDelete(tableName, metadata));
        cache.put(entityClass, tableName, SqlStatementCache.StatementType.SELECT_BY_ID,
                 generateSQLServerSelectById(tableName, metadata));
    }

    // ============= MySQL SQL Generation Methods =============

    private String generateMySQLInsert(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String placeholders = metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private String generateMySQLBatchInsert(String tableName, EntityMetadata<?> metadata, int batchSize) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String singlePlaceholder = "(" + metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", ")) + ")";

        StringJoiner valueGroups = new StringJoiner(", ");
        for (int i = 0; i < batchSize; i++) {
            valueGroups.add(singlePlaceholder);
        }

        return String.format("INSERT INTO %s (%s) VALUES %s", tableName, columns, valueGroups.toString());
    }

    private String generateMySQLUpdate(String tableName, EntityMetadata<?> metadata) {
        String setClause = metadata.getFields().stream()
            .filter(f -> !f.isId())
            .map(f -> f.getColumnName() + " = ?")
            .collect(Collectors.joining(", "));

        return String.format("UPDATE %s SET %s WHERE %s = ?",
                           tableName, setClause, metadata.getIdField().getColumnName());
    }

    private String generateMySQLDelete(String tableName, EntityMetadata<?> metadata) {
        return String.format("DELETE FROM %s WHERE %s = ?",
                           tableName, metadata.getIdField().getColumnName());
    }

    private String generateMySQLSelectById(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        return String.format("SELECT %s FROM %s WHERE %s = ?",
                           columns, tableName, metadata.getIdField().getColumnName());
    }

    // ============= PostgreSQL SQL Generation Methods =============

    private String generatePostgreSQLInsert(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String placeholders = metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
                           tableName, columns, placeholders, metadata.getIdField().getColumnName());
    }

    private String generatePostgreSQLBatchInsert(String tableName, EntityMetadata<?> metadata, int batchSize) {
        // PostgreSQL uses same syntax as MySQL for multi-row VALUES
        return generateMySQLBatchInsert(tableName, metadata, batchSize) +
               " RETURNING " + metadata.getIdField().getColumnName();
    }

    private String generatePostgreSQLUpdate(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLUpdate(tableName, metadata);
    }

    private String generatePostgreSQLDelete(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLDelete(tableName, metadata);
    }

    private String generatePostgreSQLSelectById(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLSelectById(tableName, metadata);
    }

    // ============= Oracle SQL Generation Methods =============

    private String generateOracleInsert(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String placeholders = metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private String generateOracleBatchInsert(String tableName, EntityMetadata<?> metadata, int batchSize) {
        // Oracle uses INSERT ALL syntax
        StringBuilder sql = new StringBuilder("INSERT ALL\n");

        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String placeholders = metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", "));

        for (int i = 0; i < batchSize; i++) {
            sql.append(String.format("  INTO %s (%s) VALUES (%s)\n", tableName, columns, placeholders));
        }
        sql.append("SELECT * FROM dual");

        return sql.toString();
    }

    private String generateOracleUpdate(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLUpdate(tableName, metadata);
    }

    private String generateOracleDelete(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLDelete(tableName, metadata);
    }

    private String generateOracleSelectById(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLSelectById(tableName, metadata);
    }

    // ============= SQL Server SQL Generation Methods =============

    private String generateSQLServerInsert(String tableName, EntityMetadata<?> metadata) {
        String columns = metadata.getFields().stream()
            .map(FieldMetadata::getColumnName)
            .collect(Collectors.joining(", "));

        String placeholders = metadata.getFields().stream()
            .map(f -> "?")
            .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private String generateSQLServerBatchInsert(String tableName, EntityMetadata<?> metadata, int batchSize) {
        // SQL Server 2008+ supports multi-row VALUES like MySQL
        return generateMySQLBatchInsert(tableName, metadata, batchSize);
    }

    private String generateSQLServerUpdate(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLUpdate(tableName, metadata);
    }

    private String generateSQLServerDelete(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLDelete(tableName, metadata);
    }

    private String generateSQLServerSelectById(String tableName, EntityMetadata<?> metadata) {
        // Same as MySQL
        return generateMySQLSelectById(tableName, metadata);
    }

    // ============= Retrieval Methods =============

    /**
     * Get cached SQL statement
     */
    public String getCachedSQL(PersistenceProvider.DatabaseType dbType,
                               Class<?> entityClass,
                               String tableName,
                               SqlStatementCache.StatementType statementType) {
        SqlStatementCache cache = cacheByDatabase.get(dbType);
        if (cache == null) return null;

        return cache.get(entityClass, tableName, statementType);
    }

    /**
     * Get appropriate batch statement type based on entity count
     */
    public SqlStatementCache.StatementType getBatchStatementType(int entityCount) {
        if (entityCount <= 10) {
            return SqlStatementCache.StatementType.INSERT_BATCH_10;
        } else if (entityCount <= 100) {
            return SqlStatementCache.StatementType.INSERT_BATCH_100;
        } else if (entityCount <= 1000) {
            return SqlStatementCache.StatementType.INSERT_BATCH_1000;
        } else if (entityCount <= 5000) {
            return SqlStatementCache.StatementType.INSERT_BATCH_5000;
        } else {
            // For larger batches, return null to generate at runtime
            return null;
        }
    }

    /**
     * Clear all caches
     */
    public void clearAllCaches() {
        cacheByDatabase.values().forEach(SqlStatementCache::clearAll);
        cacheByDatabase.clear();
        entityMetadataMap.clear();
    }

    /**
     * Get cache statistics for all databases
     */
    public Map<PersistenceProvider.DatabaseType, SqlStatementCache.CacheStatistics> getAllStatistics() {
        Map<PersistenceProvider.DatabaseType, SqlStatementCache.CacheStatistics> stats = new HashMap<>();
        cacheByDatabase.forEach((dbType, cache) -> stats.put(dbType, cache.getStatistics()));
        return stats;
    }
}