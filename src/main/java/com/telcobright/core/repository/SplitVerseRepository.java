package com.telcobright.core.repository;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.core.partition.PartitionStrategy;
import com.telcobright.core.partition.PartitionStrategyFactory;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.routing.HashRouter;

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
 * @param <K> Key type (String recommended for flexibility)
 */
public class SplitVerseRepository<T extends ShardingEntity> implements ShardingRepository<T> {
    
    private final Map<String, ShardingRepository<T>> shardRepositories;
    private final HashRouter router;
    private final List<ShardConfig> shardConfigs;
    private final Class<T> entityClass;
    private final String[] shardIds;
    private final ExecutorService executorService;
    private final PartitionType partitionType;
    private final String partitionKeyColumn;
    
    private SplitVerseRepository(Builder<T> builder) {
        this.entityClass = builder.entityClass;
        this.shardConfigs = builder.shardConfigs;
        this.partitionType = builder.getPartitionType();
        this.partitionKeyColumn = builder.getPartitionKeyColumn();
        this.shardRepositories = new HashMap<>();
        this.router = new HashRouter(builder.shardConfigs.size());
        this.executorService = Executors.newFixedThreadPool(
            Math.min(builder.shardConfigs.size() * 2, 10)
        );
        
        // Initialize shard repositories
        List<String> activeShardIds = new ArrayList<>();
        for (ShardConfig config : shardConfigs) {
            if (config.isEnabled()) {
                try {
                    ShardingRepository<T> shardRepo = createShardRepository(config, builder);
                    shardRepositories.put(config.getShardId(), shardRepo);
                    activeShardIds.add(config.getShardId());
                    System.out.println("[SplitVerse] Initialized shard: " + config.getShardId());
                } catch (Exception e) {
                    System.err.println("[SplitVerse] Failed to initialize shard " + 
                        config.getShardId() + ": " + e.getMessage());
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
    
    private ShardingRepository<T> createShardRepository(ShardConfig config, Builder<T> builder) {
        // For now, create GenericPartitionedTableRepository for each shard
        // In future, this can be configurable (partitioned vs multi-table)
        
        // Get table name from @Table annotation or derive from class name
        String tableName = "subscribers"; // Default for this example
        try {
            com.telcobright.core.annotation.Table tableAnnotation = 
                entityClass.getAnnotation(com.telcobright.core.annotation.Table.class);
            if (tableAnnotation != null && !tableAnnotation.name().isEmpty()) {
                tableName = tableAnnotation.name();
            }
        } catch (Exception e) {
            // Use default
        }
        
        return GenericPartitionedTableRepository.<T>builder(entityClass)
            .host(config.getHost())
            .port(config.getPort())
            .database(config.getDatabase())
            .username(config.getUsername())
            .password(config.getPassword())
            .tableName(tableName)
            .partitionRetentionPeriod(30) // Configurable in future
            .partitionType(builder.getPartitionType())
            .partitionKeyColumn(builder.getPartitionKeyColumn())
            .build();
    }
    
    private ShardingRepository<T> getShardForKey(String key) {
        if (shardRepositories.size() == 1) {
            // Optimization for single shard
            return shardRepositories.values().iterator().next();
        }
        
        String shardId = router.getShardId(key, shardIds);
        ShardingRepository<T> repo = shardRepositories.get(shardId);
        
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
        
        ShardingRepository<T> targetShard = getShardForKey(id);
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
            ShardingRepository<T> shard = shardRepositories.get(entry.getKey());
            shard.insertMultiple(entry.getValue());
        }
    }
    
    @Override
    public T findById(String id) throws SQLException {
        if (id == null) {
            return null;
        }
        
        ShardingRepository<T> targetShard = getShardForKey(id);
        return targetShard.findById(id);
    }
    
    @Override
    public List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Fan-out query to all shards
        if (shardRepositories.size() == 1) {
            // Optimization for single shard
            return shardRepositories.values().iterator().next()
                .findAllByDateRange(startDate, endDate);
        }
        
        // Parallel query execution for multiple shards
        List<CompletableFuture<List<T>>> futures = shardRepositories.values().stream()
            .map(shard -> CompletableFuture.supplyAsync(() -> {
                try {
                    return shard.findAllByDateRange(startDate, endDate);
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
    public T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Fan-out to all shards, return first found
        for (ShardingRepository<T> shard : shardRepositories.values()) {
            T result = shard.findByIdAndDateRange(startDate, endDate);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
    
    @Override
    public List<T> findAllByIdsAndDateRange(List<String> ids, LocalDateTime startDate, 
                                            LocalDateTime endDate) throws SQLException {
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
            ShardingRepository<T> shard = shardRepositories.get(entry.getKey());
            results.addAll(shard.findAllByIdsAndDateRange(entry.getValue(), startDate, endDate));
        }
        
        return results;
    }
    
    @Override
    public List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        // Fan-out query to all shards
        return fanOutQuery(shard -> shard.findAllBeforeDate(beforeDate));
    }
    
    @Override
    public List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException {
        // Fan-out query to all shards
        return fanOutQuery(shard -> shard.findAllAfterDate(afterDate));
    }
    
    @Override
    public void updateById(String id, T entity) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }
        
        ShardingRepository<T> targetShard = getShardForKey(id);
        targetShard.updateById(id, entity);
    }
    
    @Override
    public void updateByIdAndDateRange(String id, T entity, LocalDateTime startDate, 
                                       LocalDateTime endDate) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null");
        }
        
        ShardingRepository<T> targetShard = getShardForKey(id);
        targetShard.updateByIdAndDateRange(id, entity, startDate, endDate);
    }
    
    @Override
    public T findOneByIdGreaterThan(String id) throws SQLException {
        // For single shard, direct delegation
        if (shardRepositories.size() == 1) {
            return shardRepositories.values().iterator().next().findOneByIdGreaterThan(id);
        }
        
        // For multiple shards, need to query all and find minimum
        T result = null;
        for (ShardingRepository<T> shard : shardRepositories.values()) {
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
        // For single shard, direct delegation
        if (shardRepositories.size() == 1) {
            return shardRepositories.values().iterator().next()
                .findBatchByIdGreaterThan(id, batchSize);
        }
        
        // For multiple shards, need to merge results
        List<T> results = new ArrayList<>();
        for (ShardingRepository<T> shard : shardRepositories.values()) {
            if (results.size() >= batchSize) {
                break;
            }
            int remaining = batchSize - results.size();
            List<T> shardResults = shard.findBatchByIdGreaterThan(id, remaining);
            results.addAll(shardResults);
        }
        
        // Sort and limit to batchSize
        return results.stream()
            .sorted((a, b) -> compareIds(a.getId(), b.getId()))
            .limit(batchSize)
            .collect(Collectors.toList());
    }
    
    @Override
    public void shutdown() {
        System.out.println("[SplitVerse] Shutting down...");
        
        // Shutdown all shard repositories
        for (Map.Entry<String, ShardingRepository<T>> entry : shardRepositories.entrySet()) {
            try {
                entry.getValue().shutdown();
                System.out.println("[SplitVerse] Shut down shard: " + entry.getKey());
            } catch (Exception e) {
                System.err.println("[SplitVerse] Error shutting down shard " + 
                    entry.getKey() + ": " + e.getMessage());
            }
        }
        
        // Shutdown executor service
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        
        System.out.println("[SplitVerse] Shutdown complete");
    }
    
    // Helper method for fan-out queries
    private List<T> fanOutQuery(ShardQueryFunction<T> queryFunction) throws SQLException {
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
    private interface ShardQueryFunction<T extends ShardingEntity> {
        List<T> query(ShardingRepository<T> shard) throws SQLException;
    }
    
    // Builder
    public static <T extends ShardingEntity> Builder<T> builder() {
        return new Builder<>();
    }
    
    public static class Builder<T extends ShardingEntity> {
        private List<ShardConfig> shardConfigs = new ArrayList<>();
        private Class<T> entityClass;
        private PartitionType partitionType = PartitionType.DATE_BASED; // Default
        private String partitionKeyColumn = "created_at"; // Default for date-based
        
        public Builder<T> withSingleShard(ShardConfig config) {
            this.shardConfigs = Collections.singletonList(config);
            return this;
        }
        
        public Builder<T> withShardConfigs(List<ShardConfig> configs) {
            this.shardConfigs = configs;
            return this;
        }
        
        public Builder<T> withEntityClass(Class<T> entityClass) {
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
        public Builder<T> withPartitionType(PartitionType partitionType) {
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
        public Builder<T> withPartitionKeyColumn(String columnName) {
            if (columnName == null || columnName.trim().isEmpty()) {
                throw new IllegalArgumentException("Partition key column cannot be null or empty");
            }
            this.partitionKeyColumn = columnName;
            return this;
        }
        
        
        public SplitVerseRepository<T> build() {
            if (shardConfigs.isEmpty()) {
                throw new IllegalArgumentException("At least one shard configuration is required");
            }
            if (entityClass == null) {
                throw new IllegalArgumentException("Entity class is required");
            }
            
            // Validate partition type is supported
            partitionType.validateSupported();
            
            // Log the configuration
            System.out.println("[SplitVerse] Building repository with:");
            System.out.println("  Partition Type: " + partitionType);
            System.out.println("  Partition Key Column: " + partitionKeyColumn);
            System.out.println("  Shards: " + shardConfigs.size());
            
            return new SplitVerseRepository<>(this);
        }
        
        // Package-private getters for SplitVerseRepository constructor
        PartitionType getPartitionType() { return partitionType; }
        String getPartitionKeyColumn() { return partitionKeyColumn; }
    }
}