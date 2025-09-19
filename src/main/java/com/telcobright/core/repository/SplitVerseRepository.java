package com.telcobright.core.repository;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.core.partition.PartitionStrategy;
import com.telcobright.core.partition.PartitionStrategyFactory;
import com.telcobright.core.enums.ShardingStrategy;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.config.DataSourceConfig;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.config.RepositoryMode;
import com.telcobright.splitverse.routing.HashRouter;
import com.telcobright.core.sql.SqlGeneratorByEntityRegistry;
import com.telcobright.core.sql.SqlStatementCache;
import com.telcobright.core.persistence.PersistenceProvider;
import com.telcobright.core.persistence.MySQLPersistenceProvider;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Split-Verse Repository: Infinite horizontal sharding layer.
 *
 * Phase 1 Implementation: Single shard wrapper with hash routing ready for expansion.
 *
 * @param <T> Entity type implementing ShardingEntity
 * @param <P> Partition column value type (must be Comparable)
 */
public class SplitVerseRepository<T extends ShardingEntity<P>, P extends Comparable<? super P>> implements ShardingRepository<T, P> {
    
    private final Map<String, ShardingRepository<T, P>> shardRepositories;
    private final HashRouter router;
    private final List<ShardConfig> shardConfigs;
    private final Class<T> entityClass;
    private final String[] shardIds;
    private final ExecutorService executorService;
    private final PartitionType partitionType;
    private final String partitionKeyColumn;
    private final SqlGeneratorByEntityRegistry sqlRegistry;
    private final SqlStatementCache sqlCache;
    
    private SplitVerseRepository(Builder<T, P> builder) {
        this.entityClass = builder.entityClass;
        this.shardConfigs = builder.shardConfigs;
        this.partitionType = builder.getPartitionType();
        this.partitionKeyColumn = builder.getPartitionKeyColumn();
        this.shardRepositories = new HashMap<>();
        this.router = new HashRouter(builder.shardConfigs.size());
        this.executorService = Executors.newFixedThreadPool(
            Math.min(builder.shardConfigs.size() * 2, 10)
        );
        this.sqlRegistry = builder.sqlRegistry;
        this.sqlCache = builder.sqlCache != null ? builder.sqlCache :
            (sqlRegistry != null ? sqlRegistry.getCache() : new SqlStatementCache());
        
        // Initialize shard repositories
        List<String> activeShardIds = new ArrayList<>();
        for (ShardConfig config : shardConfigs) {
            if (config.isEnabled()) {
                try {
                    ShardingRepository<T, P> shardRepo = createShardRepository(config, builder);
                    shardRepositories.put(config.getShardId(), shardRepo);
                    activeShardIds.add(config.getShardId());
                    System.out.println("[SplitVerse] Initialized shard: " + config.getShardId());
                } catch (Exception e) {
                    System.err.println("[SplitVerse] Failed to initialize shard " +
                        config.getShardId() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        
        this.shardIds = activeShardIds.toArray(new String[0]);
        
        if (shardRepositories.isEmpty()) {
            throw new IllegalStateException("No shards could be initialized");
        }
        
        System.out.println("[SplitVerse] Initialized with " + shardRepositories.size() + 
            " active shard(s)");
    }
    
    private ShardingRepository<T, P> createShardRepository(ShardConfig config, Builder<T, P> builder) {
        // Pass SQL cache to repositories if available
        PersistenceProvider persistenceProvider = null;
        if (sqlCache != null) {
            persistenceProvider = new MySQLPersistenceProvider(sqlCache);
        }
        // Use table name from builder or annotation
        String tableName = builder.tableName;
        if (tableName == null || tableName.isEmpty()) {
            // Try to get from annotation
            try {
                com.telcobright.core.annotation.Table tableAnnotation =
                    entityClass.getAnnotation(com.telcobright.core.annotation.Table.class);
                if (tableAnnotation != null && !tableAnnotation.name().isEmpty()) {
                    tableName = tableAnnotation.name();
                } else {
                    // Default from class name
                    tableName = entityClass.getSimpleName().toLowerCase() + "s";
                }
            } catch (Exception e) {
                tableName = entityClass.getSimpleName().toLowerCase() + "s";
            }
        }

        // Choose repository type based on mode
        if (builder.getRepositoryMode() == RepositoryMode.MULTI_TABLE) {
            // Create multi-table repository with proper partition support
            return GenericMultiTableRepository.<T, P>builder(entityClass)
                .host(config.getHost())
                .port(config.getPort())
                .database(config.getDatabase())
                .username(config.getUsername())
                .password(config.getPassword())
                .baseTableName(tableName)
                .tableRetentionDays(builder.getRetentionDays())
                .tableGranularity(builder.getTableGranularity())
                .partitionRange(builder.getPartitionRange())
                .partitionColumn(builder.getPartitionColumn())
                .partitionColumnType(builder.getPartitionColumnType())
                .autoCreateTables(true)
                .charset(builder.getCharset())
                .collation(builder.getCollation())
                .persistenceProvider(persistenceProvider)
                .build();
        } else {
            // Create partitioned repository (single table with partitions)
            return GenericPartitionedTableRepository.<T, P>builder(entityClass)
                .host(config.getHost())
                .port(config.getPort())
                .database(config.getDatabase())
                .username(config.getUsername())
                .password(config.getPassword())
                .tableName(tableName)
                .partitionRetentionPeriod(builder.getRetentionDays())
                .withPartitionType(builder.getPartitionType())
                .withPartitionKeyColumn(builder.getPartitionKeyColumn())
                .charset(builder.getCharset())
                .collation(builder.getCollation())
                .build();
        }
    }
    
    private ShardingRepository<T, P> getShardForKey(String key) {
        if (shardRepositories.size() == 1) {
            // Optimization for single shard
            return shardRepositories.values().iterator().next();
        }
        
        String shardId = router.getShardId(key, shardIds);
        ShardingRepository<T, P> repo = shardRepositories.get(shardId);
        
        if (repo == null) {
            throw new IllegalStateException("Shard not available: " + shardId);
        }
        
        return repo;
    }
    
    @Override
    public void insert(T entity) throws SQLException {
        String id = entity.getId();
        if (id == null) {
            throw new IllegalArgumentException("Entity ID cannot be null. IDs must be generated externally.");
        }
        
        ShardingRepository<T, P> targetShard = getShardForKey(id);
        targetShard.insert(entity);
    }
    
    @Override
    public void insertMultiple(List<T> entities) throws SQLException {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        // Group entities by shard
        Map<String, List<T>> entitiesByShard = new HashMap<>();
        for (T entity : entities) {
            String id = entity.getId();
            if (id == null) {
                throw new IllegalArgumentException("Entity ID cannot be null");
            }
            
            String shardId = router.getShardId(id, shardIds);
            entitiesByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(entity);
        }
        
        // Insert into each shard (can be parallelized in future)
        for (Map.Entry<String, List<T>> entry : entitiesByShard.entrySet()) {
            ShardingRepository<T, P> shard = shardRepositories.get(entry.getKey());
            shard.insertMultiple(entry.getValue());
        }
    }
    
    @Override
    public T findById(String id) throws SQLException {
        if (id == null) {
            return null;
        }
        
        ShardingRepository<T, P> targetShard = getShardForKey(id);
        return targetShard.findById(id);
    }
    
    @Override
    public List<T> findAllByPartitionRange(P startValue, P endValue) throws SQLException {
        // Fan-out query to all shards
        if (shardRepositories.size() == 1) {
            // Optimization for single shard
            return shardRepositories.values().iterator().next()
                .findAllByPartitionRange(startValue, endValue);
        }
        
        // Parallel query execution for multiple shards
        List<CompletableFuture<List<T>>> futures = shardRepositories.values().stream()
            .map(shard -> CompletableFuture.supplyAsync(() -> {
                try {
                    return shard.findAllByPartitionRange(startValue, endValue);
                } catch (SQLException e) {
                    throw new CompletionException(e);
                }
            }, executorService))
            .collect(Collectors.toList());
        
        try {
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
                    .collect(Collectors.toList()))
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SQLException("Failed to query shards", e);
        }
    }
    
    @Override
    public T findByIdAndPartitionRange(String id, P startValue, P endValue) throws SQLException {
        // Fan-out to all shards, return first found
        for (ShardingRepository<T, P> shard : shardRepositories.values()) {
            T result = shard.findByIdAndPartitionRange(id, startValue, endValue);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
    
    @Override
    public List<T> findAllByIdsAndPartitionRange(List<String> ids, P startValue,
                                                  P endValue) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Group IDs by shard
        Map<String, List<String>> idsByShard = new HashMap<>();
        for (String id : ids) {
            String shardId = router.getShardId(id, shardIds);
            idsByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(id);
        }
        
        // Query each shard with its IDs
        List<T> results = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : idsByShard.entrySet()) {
            ShardingRepository<T, P> shard = shardRepositories.get(entry.getKey());
            results.addAll(shard.findAllByIdsAndPartitionRange(entry.getValue(), startValue, endValue));
        }
        
        return results;
    }
    
    @Override
    public List<T> findAllBeforePartitionValue(P beforeValue) throws SQLException {
        // Fan-out query to all shards
        return fanOutQuery(shard -> shard.findAllBeforePartitionValue(beforeValue));
    }
    
    @Override
    public List<T> findAllAfterPartitionValue(P afterValue) throws SQLException {
        // Fan-out query to all shards
        return fanOutQuery(shard -> shard.findAllAfterPartitionValue(afterValue));
    }
    
    @Override
    public void updateById(String id, T entity) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }
        
        ShardingRepository<T, P> targetShard = getShardForKey(id);
        targetShard.updateById(id, entity);
    }
    
    @Override
    public void updateByIdAndPartitionRange(String id, T entity, P startValue,
                                            P endValue) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }
        
        ShardingRepository<T, P> targetShard = getShardForKey(id);
        targetShard.updateByIdAndPartitionRange(id, entity, startValue, endValue);
    }
    
    @Override
    public T findOneByIdGreaterThan(String id) throws SQLException {
        // For single shard, direct delegation
        if (shardRepositories.size() == 1) {
            return shardRepositories.values().iterator().next().findOneByIdGreaterThan(id);
        }
        
        // For multiple shards, need to query all and find minimum
        T result = null;
        for (ShardingRepository<T, P> shard : shardRepositories.values()) {
            T candidate = shard.findOneByIdGreaterThan(id);
            if (candidate != null) {
                if (result == null || compareIds(candidate.getId(), result.getId()) < 0) {
                    result = candidate;
                }
            }
        }
        return result;
    }
    
    @Override
    public List<T> findBatchByIdGreaterThan(String id, int batchSize) throws SQLException {
        // For single shard, direct delegation with optimization
        if (shardRepositories.size() == 1) {
            // Single shard: can use direct LIMIT/OFFSET in SQL for efficiency
            return shardRepositories.values().iterator().next()
                .findBatchByIdGreaterThan(id, batchSize);
        }

        // For multiple shards, proper cross-shard pagination is complex
        throw new UnsupportedOperationException(
            "Cross-shard pagination is not yet implemented for multiple shards. " +
            "Current configuration has " + shardRepositories.size() + " shards. " +
            "Please use a single shard configuration or implement cursor-based pagination."
        );

        /* Future implementation for multi-shard pagination:
         * 1. Query each shard with: SELECT * WHERE id > ? ORDER BY id LIMIT (offset + batchSize)
         * 2. Merge all results maintaining sort order
         * 3. Apply pagination window to get the requested batch
         * 4. Consider using CrossShardPaginator class for this
         */
    }
    
    @Override
    public void deleteById(String id) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }

        ShardingRepository<T, P> targetShard = getShardForKey(id);
        targetShard.deleteById(id);
    }

    @Override
    public void deleteByIdAndPartitionRange(String id, P startValue, P endValue) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }

        ShardingRepository<T, P> targetShard = getShardForKey(id);
        targetShard.deleteByIdAndPartitionRange(id, startValue, endValue);
    }

    @Override
    public void deleteAllByPartitionRange(P startValue, P endValue) throws SQLException {
        // Fan-out delete to all shards
        for (ShardingRepository<T, P> shard : shardRepositories.values()) {
            shard.deleteAllByPartitionRange(startValue, endValue);
        }
    }

    @Override
    public void shutdown() {
        System.out.println("[SplitVerse] Shutting down...");
        
        // Shutdown all shard repositories
        for (Map.Entry<String, ShardingRepository<T, P>> entry : shardRepositories.entrySet()) {
            try {
                entry.getValue().shutdown();
                System.out.println("[SplitVerse] Shut down shard: " + entry.getKey());
            } catch (Exception e) {
                System.err.println("[SplitVerse] Error shutting down shard " + 
                    entry.getKey() + ": " + e.getMessage());
            }
        }
        
        // Shutdown executor service immediately
        executorService.shutdownNow();
        try {
            // Wait only 1 second for termination
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("[SplitVerse] Shutdown complete");
    }
    
    // Helper method for fan-out queries
    private List<T> fanOutQuery(ShardQueryFunction<T, P> queryFunction) throws SQLException {
        if (shardRepositories.size() == 1) {
            return queryFunction.query(shardRepositories.values().iterator().next());
        }
        
        List<CompletableFuture<List<T>>> futures = shardRepositories.values().stream()
            .map(shard -> CompletableFuture.supplyAsync(() -> {
                try {
                    return queryFunction.query(shard);
                } catch (SQLException e) {
                    throw new CompletionException(e);
                }
            }, executorService))
            .collect(Collectors.toList());
        
        try {
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
                    .collect(Collectors.toList()))
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SQLException("Failed to query shards", e);
        }
    }
    
    
    // Helper method to compare String IDs
    private int compareIds(String id1, String id2) {
        return id1.compareTo(id2);
    }
    
    // Functional interface for shard queries
    @FunctionalInterface
    private interface ShardQueryFunction<T extends ShardingEntity<P>, P extends Comparable<? super P>> {
        List<T> query(ShardingRepository<T, P> shard) throws SQLException;
    }
    
    // Builder
    public static <T extends ShardingEntity<P>, P extends Comparable<? super P>> Builder<T, P> builder() {
        return new Builder<T, P>();
    }
    
    public static class Builder<T extends ShardingEntity<P>, P extends Comparable<? super P>> {
        private List<ShardConfig> shardConfigs = new ArrayList<>();
        private Class<T> entityClass;
        private String tableName;
        private RepositoryMode repositoryMode = RepositoryMode.MULTI_TABLE; // Default to multi-table

        // New design fields
        private ShardingStrategy shardingStrategy = ShardingStrategy.SINGLE_KEY_HASH; // Default
        private String partitionColumn;
        private PartitionColumnType partitionColumnType;
        private PartitionRange partitionRange;

        // Legacy fields for backward compatibility
        private PartitionType partitionType = PartitionType.DATE_BASED; // Default for partitioned mode
        private String partitionKeyColumn = "created_at"; // Default for date-based
        private GenericMultiTableRepository.TableGranularity tableGranularity =
            GenericMultiTableRepository.TableGranularity.DAILY; // Default for multi-table mode
        private int retentionDays = 30; // Default retention period
        private String charset = "utf8mb4"; // Default charset
        private String collation = "utf8mb4_bin"; // Default collation
        private int idSize = 22; // Default ID size
        private SqlGeneratorByEntityRegistry sqlRegistry;
        private SqlStatementCache sqlCache;
        private PersistenceProvider.DatabaseType databaseType = PersistenceProvider.DatabaseType.MYSQL;
        private boolean pregenerateSql = false;
        
        public Builder<T, P> withSingleShard(ShardConfig config) {
            this.shardConfigs = Collections.singletonList(config);
            return this;
        }
        
        public Builder<T, P> withShardConfigs(List<ShardConfig> configs) {
            this.shardConfigs = configs;
            return this;
        }
        
        public Builder<T, P> withEntityClass(Class<T> entityClass) {
            this.entityClass = entityClass;
            return this;
        }
        
        /**
         * Set the partition type for tables within each shard.
         * Default is DATE_BASED. Other types are not yet implemented.
         * 
         * @param partitionType Type of partitioning to use
         * @return Builder instance
         * @throws UnsupportedOperationException if partition type is not implemented
         */
        public Builder<T, P> withPartitionType(PartitionType partitionType) {
            if (partitionType == null) {
                throw new IllegalArgumentException("Partition type cannot be null");
            }
            // Validate that the partition type is supported
            partitionType.validateSupported();
            this.partitionType = partitionType;
            return this;
        }
        
        /**
         * Set the column to use for partitioning.
         * For DATE_BASED, this should be a LocalDateTime column (default: "created_at").
         * 
         * @param columnName Name of the column to partition by
         * @return Builder instance
         */
        public Builder<T, P> withPartitionKeyColumn(String columnName) {
            if (columnName == null || columnName.trim().isEmpty()) {
                throw new IllegalArgumentException("Partition key column cannot be null or empty");
            }
            this.partitionKeyColumn = columnName;
            return this;
        }

        /**
         * Set the repository mode (PARTITIONED or MULTI_TABLE).
         * Default is PARTITIONED.
         */
        public Builder<T, P> withRepositoryMode(RepositoryMode mode) {
            if (mode == null) {
                throw new IllegalArgumentException("Repository mode cannot be null");
            }
            this.repositoryMode = mode;
            return this;
        }

        /**
         * Set the table granularity for multi-table mode.
         * Only applies when repository mode is MULTI_TABLE.
         */
        public Builder<T, P> withTableGranularity(GenericMultiTableRepository.TableGranularity granularity) {
            if (granularity == null) {
                throw new IllegalArgumentException("Table granularity cannot be null");
            }
            this.tableGranularity = granularity;
            return this;
        }

        /**
         * Set the retention period in days.
         * Applies to both partitioned and multi-table modes.
         */
        public Builder<T, P> withRetentionDays(int days) {
            if (days < 1) {
                throw new IllegalArgumentException("Retention days must be at least 1");
            }
            this.retentionDays = days;
            return this;
        }

        /**
         * Set the character set for tables.
         */
        public Builder<T, P> withCharset(String charset) {
            if (charset != null && !charset.trim().isEmpty()) {
                this.charset = charset;
            }
            return this;
        }

        /**
         * Set the collation for tables.
         */
        public Builder<T, P> withCollation(String collation) {
            if (collation != null && !collation.trim().isEmpty()) {
                this.collation = collation;
            }
            return this;
        }

        // New API methods

        /**
         * Set the table name for the entities.
         */
        public Builder<T, P> withTableName(String tableName) {
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }
            this.tableName = tableName;
            return this;
        }

        /**
         * Set the sharding strategy.
         */
        public Builder<T, P> withShardingStrategy(ShardingStrategy strategy) {
            if (strategy == null) {
                throw new IllegalArgumentException("Sharding strategy cannot be null");
            }
            this.shardingStrategy = strategy;
            return this;
        }

        /**
         * Set the partition column name and type.
         * Required for DUAL_KEY strategies.
         */
        public Builder<T, P> withPartitionColumn(String columnName, PartitionColumnType columnType) {
            if (shardingStrategy != ShardingStrategy.SINGLE_KEY_HASH) {
                if (columnName == null || columnName.trim().isEmpty()) {
                    throw new IllegalArgumentException("Partition column name required for DUAL_KEY strategies");
                }
                if (columnType == null) {
                    throw new IllegalArgumentException("Partition column type required for DUAL_KEY strategies");
                }
            }
            this.partitionColumn = columnName;
            this.partitionColumnType = columnType;

            // Set legacy fields for compatibility
            this.partitionKeyColumn = columnName;
            return this;
        }

        /**
         * Set the partition range.
         */
        public Builder<T, P> withPartitionRange(PartitionRange range) {
            if (range == null) {
                throw new IllegalArgumentException("Partition range cannot be null");
            }
            this.partitionRange = range;

            // Map to legacy table granularity for compatibility
            if (range == PartitionRange.HOURLY) {
                this.tableGranularity = GenericMultiTableRepository.TableGranularity.HOURLY;
            } else if (range == PartitionRange.DAILY) {
                this.tableGranularity = GenericMultiTableRepository.TableGranularity.DAILY;
            } else if (range == PartitionRange.MONTHLY) {
                this.tableGranularity = GenericMultiTableRepository.TableGranularity.MONTHLY;
            }
            return this;
        }

        /**
         * Set the ID field size (8-22 bytes).
         */
        public Builder<T, P> withIdSize(int size) {
            if (size < 8 || size > 22) {
                throw new IllegalArgumentException("ID size must be between 8 and 22 bytes");
            }
            this.idSize = size;
            return this;
        }

        /**
         * Configure data sources for sharding.
         */
        public Builder<T, P> withDataSources(List<DataSourceConfig> dataSources) {
            if (dataSources == null || dataSources.isEmpty()) {
                throw new IllegalArgumentException("At least one data source is required");
            }

            // Convert DataSourceConfig to ShardConfig
            this.shardConfigs = new ArrayList<>();
            for (int i = 0; i < dataSources.size(); i++) {
                DataSourceConfig ds = dataSources.get(i);
                ShardConfig shardConfig = ShardConfig.builder()
                    .shardId("shard-" + i)
                    .host(ds.getHost())
                    .port(ds.getPort())
                    .database(ds.getDatabase())
                    .username(ds.getUsername())
                    .password(ds.getPassword())
                    .enabled(true)
                    .build();
                this.shardConfigs.add(shardConfig);
            }
            return this;
        }

        /**
         * Enable SQL pre-generation for better performance.
         * SQL statements will be generated at startup for batch sizes: 10, 100, 1000, 5000.
         */
        public Builder<T, P> withSqlPregeneration() {
            this.pregenerateSql = true;
            return this;
        }

        /**
         * Set the database type for SQL generation.
         * Default is MySQL.
         */
        public Builder<T, P> withDatabaseType(PersistenceProvider.DatabaseType dbType) {
            if (dbType == null) {
                throw new IllegalArgumentException("Database type cannot be null");
            }
            this.databaseType = dbType;
            return this;
        }

        /**
         * Provide a custom SQL statement cache.
         * If not provided and SQL pre-generation is enabled, a new cache will be created.
         */
        public Builder<T, P> withSqlCache(SqlStatementCache cache) {
            this.sqlCache = cache;
            return this;
        }

        /**
         * Provide a custom SQL generator registry.
         * This allows sharing the registry across multiple repositories.
         */
        public Builder<T, P> withSqlRegistry(SqlGeneratorByEntityRegistry registry) {
            this.sqlRegistry = registry;
            return this;
        }

        public SplitVerseRepository<T, P> build() {
            // Validate required fields
            if (shardConfigs.isEmpty()) {
                throw new IllegalArgumentException("At least one shard configuration is required");
            }
            if (entityClass == null) {
                throw new IllegalArgumentException("Entity class is required");
            }

            // Validate sharding strategy requirements
            if (shardingStrategy != ShardingStrategy.SINGLE_KEY_HASH) {
                if (partitionColumn == null || partitionColumn.trim().isEmpty()) {
                    throw new IllegalArgumentException("Partition column required for " + shardingStrategy);
                }
                if (partitionColumnType == null) {
                    throw new IllegalArgumentException("Partition column type required for " + shardingStrategy);
                }
                if (partitionRange == null) {
                    throw new IllegalArgumentException("Partition range required for " + shardingStrategy);
                }
            }

            // Validate partition type is supported (legacy)
            partitionType.validateSupported();

            // Log the configuration
            System.out.println("[SplitVerse] Building repository with:");
            System.out.println("  Entity: " + entityClass.getSimpleName());
            System.out.println("  Table: " + (tableName != null ? tableName : "auto-detect"));
            System.out.println("  Sharding Strategy: " + shardingStrategy);
            System.out.println("  Repository Mode: " + repositoryMode);

            if (shardingStrategy != ShardingStrategy.SINGLE_KEY_HASH) {
                System.out.println("  Partition Column: " + partitionColumn + " (" + partitionColumnType + ")");
                System.out.println("  Partition Range: " + partitionRange);
            }

            if (repositoryMode == RepositoryMode.PARTITIONED) {
                System.out.println("  Partition Type: " + partitionType);
                System.out.println("  Partition Key Column: " + partitionKeyColumn);
            } else {
                System.out.println("  Table Granularity: " + tableGranularity);
            }

            System.out.println("  Retention Days: " + retentionDays);
            System.out.println("  ID Size: " + idSize + " bytes");
            System.out.println("  Charset: " + charset + ", Collation: " + collation);
            System.out.println("  Shards: " + shardConfigs.size());

            // Handle SQL pre-generation if enabled
            if (pregenerateSql) {
                if (sqlRegistry == null) {
                    sqlRegistry = new SqlGeneratorByEntityRegistry();
                }
                if (sqlCache == null) {
                    sqlCache = new SqlStatementCache();
                }
                sqlRegistry.setCache(sqlCache);

                // Register entity with pre-generation
                Map<Class<?>, List<String>> entityTableMap = new HashMap<>();
                List<String> tableNames = determineTableNames();
                entityTableMap.put(entityClass, tableNames);

                System.out.println("[SplitVerse] Pre-generating SQL for " + entityClass.getSimpleName() +
                    " with " + tableNames.size() + " table pattern(s)");
                sqlRegistry.pregenerateForTables(databaseType, entityTableMap);
                System.out.println("[SplitVerse] SQL pre-generation complete");
            }

            return new SplitVerseRepository<T, P>(this);
        }
        
        // Package-private getters for SplitVerseRepository constructor
        RepositoryMode getRepositoryMode() { return repositoryMode; }
        PartitionType getPartitionType() { return partitionType; }
        String getPartitionKeyColumn() { return partitionKeyColumn; }
        GenericMultiTableRepository.TableGranularity getTableGranularity() { return tableGranularity; }
        int getRetentionDays() { return retentionDays; }
        String getCharset() { return charset; }
        String getCollation() { return collation; }
        PartitionRange getPartitionRange() { return partitionRange; }

        /**
         * Determine table names based on configuration.
         * Used for SQL pre-generation.
         */
        private List<String> determineTableNames() {
            List<String> tableNames = new ArrayList<>();
            String baseTable = tableName != null ? tableName : entityClass.getSimpleName().toLowerCase() + "s";

            if (repositoryMode == RepositoryMode.MULTI_TABLE) {
                // For multi-table mode, generate patterns based on partition range
                if (partitionRange != null) {
                    switch (partitionRange) {
                        case HOURLY:
                        case DAILY:
                        case MONTHLY:
                            // Date-based patterns
                            tableNames.add(baseTable + "_2025_09_19"); // Example daily pattern
                            break;
                        case VALUE_RANGE_10K:
                            // Value range patterns
                            for (int i = 0; i < 10; i++) {
                                int start = i * 10000;
                                int end = start + 9999;
                                tableNames.add(baseTable + "_" + start + "_" + end);
                            }
                            break;
                        case VALUE_RANGE_100K:
                            // Generate first 5 ranges as examples
                            for (int i = 0; i < 5; i++) {
                                int start = i * 100000;
                                int end = start + 99999;
                                tableNames.add(baseTable + "_" + start + "_" + end);
                            }
                            break;
                        case HASH_4:
                            for (int i = 0; i < 4; i++) {
                                tableNames.add(baseTable + "_hash_" + i);
                            }
                            break;
                        case HASH_8:
                            for (int i = 0; i < 8; i++) {
                                tableNames.add(baseTable + "_hash_" + i);
                            }
                            break;
                        case HASH_16:
                            for (int i = 0; i < 16; i++) {
                                tableNames.add(baseTable + "_hash_" + i);
                            }
                            break;
                        default:
                            tableNames.add(baseTable);
                    }
                } else {
                    // Default to base table if no partition range specified
                    tableNames.add(baseTable);
                }
            } else {
                // For partitioned mode, single table
                tableNames.add(baseTable);
            }

            return tableNames;
        }
        String getPartitionColumn() { return partitionColumn; }
        PartitionColumnType getPartitionColumnType() { return partitionColumnType; }
    }
}