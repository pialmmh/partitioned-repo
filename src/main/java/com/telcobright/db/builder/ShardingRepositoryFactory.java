package com.telcobright.db.builder;

import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.sharding.ShardingMode;
import com.zaxxer.hikari.HikariConfig;

import javax.sql.DataSource;
import java.time.LocalTime;
import java.util.function.Function;

/**
 * Factory class for creating ShardingRepository instances with common configurations
 */
public class ShardingRepositoryFactory {
    
    /**
     * Creates a ShardingRepository with default settings:
     * - Partitioned table mode
     * - 7 days retention
     * - Auto-management enabled
     * - 4:00 AM partition adjustment time
     */
    public static <T> ShardingRepository<T> create(
            String entityName,
            DataSource dataSource,
            Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        
        return new ShardingRepositoryBuilder<T>()
                .entityName(entityName)
                .dataSource(dataSource)
                .withRepositoryFactory(repositoryFactory)
                .build();
    }
    
    /**
     * Creates a ShardingRepository with HikariConfig
     */
    public static <T> ShardingRepository<T> createWithHikari(
            String entityName,
            HikariConfig hikariConfig,
            Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        
        return new ShardingRepositoryBuilder<T>()
                .entityName(entityName)
                .hikariConfig(hikariConfig)
                .withRepositoryFactory(repositoryFactory)
                .build();
    }
    
    /**
     * Creates a ShardingRepository with custom retention days
     */
    public static <T> ShardingRepository<T> createWithRetention(
            String entityName,
            DataSource dataSource,
            int retentionDays,
            Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        
        return new ShardingRepositoryBuilder<T>()
                .entityName(entityName)
                .dataSource(dataSource)
                .retentionSpanDays(retentionDays)
                .withRepositoryFactory(repositoryFactory)
                .build();
    }
    
    /**
     * Creates a multi-table ShardingRepository
     */
    public static <T> ShardingRepository<T> createMultiTable(
            String entityName,
            DataSource dataSource,
            int retentionDays,
            Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        
        return new ShardingRepositoryBuilder<T>()
                .mode(ShardingMode.MULTI_TABLE)
                .entityName(entityName)
                .dataSource(dataSource)
                .retentionSpanDays(retentionDays)
                .withRepositoryFactory(repositoryFactory)
                .build();
    }
    
    /**
     * Creates a ShardingRepository with full custom configuration
     */
    public static <T> ShardingRepository<T> createCustom(
            ShardingMode mode,
            String entityName,
            DataSource dataSource,
            String shardKey,
            int retentionDays,
            LocalTime adjustmentTime,
            boolean autoManage,
            Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        
        return new ShardingRepositoryBuilder<T>()
                .mode(mode)
                .entityName(entityName)
                .dataSource(dataSource)
                .shardKey(shardKey)
                .retentionSpanDays(retentionDays)
                .partitionAdjustmentTime(adjustmentTime)
                .autoManagePartition(autoManage)
                .withRepositoryFactory(repositoryFactory)
                .build();
    }
}