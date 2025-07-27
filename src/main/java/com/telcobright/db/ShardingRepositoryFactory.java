package com.telcobright.db;

import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.scheduler.PartitionManagementService;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating ShardingRepository instances
 * Framework-agnostic - accepts both actual and ShardingSphere DataSources
 */
public class ShardingRepositoryFactory {
    
    private final DataSource actualDataSource;
    private final DataSource shardingSphereDataSource;
    private final ConcurrentHashMap<Class<?>, ShardingRepository<?>> repositoryCache = new ConcurrentHashMap<>();
    private final PartitionManagementService partitionService;
    
    /**
     * Create factory with pre-configured ShardingSphere DataSource
     * 
     * @param actualDataSource The actual MySQL DataSource (for table management)
     * @param shardingSphereDataSource The ShardingSphere wrapped DataSource (for queries)
     */
    public ShardingRepositoryFactory(DataSource actualDataSource, DataSource shardingSphereDataSource) {
        this.actualDataSource = actualDataSource;
        this.shardingSphereDataSource = shardingSphereDataSource;
        this.partitionService = new PartitionManagementService(actualDataSource);
    }
    
    /**
     * Get or create a ShardingRepository for the given entity class
     * Automatically registers entity for partition management if autoManagePartition=true
     */
    @SuppressWarnings("unchecked")
    public <T> ShardingRepository<T> getRepository(Class<T> entityClass) {
        return (ShardingRepository<T>) repositoryCache.computeIfAbsent(
            entityClass, 
            clazz -> {
                // Register entity for partition management (if autoManagePartition=true)
                partitionService.registerEntity(entityClass);
                
                // Create repository
                return new ShardingRepository<>(shardingSphereDataSource, entityClass);
            }
        );
    }
    
    /**
     * Create a new ShardingRepository (not cached, but still registers for partition management)
     */
    public <T> ShardingRepository<T> createRepository(Class<T> entityClass) {
        partitionService.registerEntity(entityClass);
        return new ShardingRepository<>(shardingSphereDataSource, entityClass);
    }
    
    /**
     * Get partition management service for manual operations
     */
    public PartitionManagementService getPartitionService() {
        return partitionService;
    }
    
    /**
     * Clear repository cache
     */
    public void clearCache() {
        repositoryCache.clear();
    }
    
    /**
     * Perform daily maintenance on all cached repositories
     */
    public void performDailyMaintenanceOnAll() {
        partitionService.triggerMaintenanceForAll();
    }
    
    /**
     * Shutdown factory and all partition schedulers
     */
    public void shutdown() {
        partitionService.shutdown();
        repositoryCache.clear();
    }
    
    /**
     * Print status of all managed partitions
     */
    public void printPartitionStatus() {
        partitionService.printStatus();
    }
}