package com.telcobright.db.builder;

import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.service.PartitionSchedulerService;
import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.sharding.ShardingMode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ShardingRepositoryBuilder<T> {
    
    private static final Logger logger = LoggerFactory.getLogger(ShardingRepositoryBuilder.class);
    
    private ShardingMode mode = ShardingMode.PARTITIONED_TABLE;
    private String entityName;
    private String shardKey = "created_at";
    private int retentionSpanDays = 7;
    private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0);
    private String shardingAlgorithmClass;
    private DataSource dataSource;
    private HikariConfig hikariConfig;
    private boolean autoManagePartition = true;
    private Function<ShardingConfig, ShardingRepository<T>> repositoryFactory;
    
    private static final List<PartitionSchedulerService> activeSchedulers = new ArrayList<>();
    private static final List<HikariDataSource> managedDataSources = new ArrayList<>();
    
    public ShardingRepositoryBuilder<T> mode(ShardingMode mode) {
        this.mode = mode;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> entityName(String entityName) {
        this.entityName = entityName;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> shardKey(String shardKey) {
        this.shardKey = shardKey;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> retentionSpanDays(int retentionSpanDays) {
        this.retentionSpanDays = retentionSpanDays;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> partitionAdjustmentTime(LocalTime partitionAdjustmentTime) {
        this.partitionAdjustmentTime = partitionAdjustmentTime;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> shardingAlgorithmClass(String shardingAlgorithmClass) {
        this.shardingAlgorithmClass = shardingAlgorithmClass;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> dataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.hikariConfig = null; // Clear hikariConfig if dataSource is set directly
        return this;
    }
    
    public ShardingRepositoryBuilder<T> hikariConfig(HikariConfig hikariConfig) {
        this.hikariConfig = hikariConfig;
        this.dataSource = null; // Clear dataSource if hikariConfig is set
        return this;
    }
    
    public ShardingRepositoryBuilder<T> autoManagePartition(boolean autoManagePartition) {
        this.autoManagePartition = autoManagePartition;
        return this;
    }
    
    public ShardingRepositoryBuilder<T> withRepositoryFactory(Function<ShardingConfig, ShardingRepository<T>> repositoryFactory) {
        this.repositoryFactory = repositoryFactory;
        return this;
    }
    
    public ShardingRepository<T> build() {
        validateParameters();
        
        // Create DataSource from HikariConfig if provided
        if (hikariConfig != null && dataSource == null) {
            HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
            dataSource = hikariDataSource;
            managedDataSources.add(hikariDataSource);
            logger.info("Created HikariDataSource for entity: {}", entityName);
        }
        
        // Build the configuration
        ShardingConfig config = ShardingConfig.builder()
                .mode(mode)
                .entityName(entityName)
                .shardKey(shardKey)
                .retentionSpanDays(retentionSpanDays)
                .partitionAdjustmentTime(partitionAdjustmentTime)
                .shardingAlgorithmClass(shardingAlgorithmClass)
                .dataSource(dataSource)
                .autoManagePartition(autoManagePartition)
                .build();
        
        // Create the repository
        ShardingRepository<T> repository = repositoryFactory.apply(config);
        
        // Initialize partitions automatically
        logger.info("Initializing partitions for entity: {}", entityName);
        repository.initializePartitions();
        
        // Start scheduler if auto-management is enabled
        if (autoManagePartition) {
            PartitionSchedulerService scheduler = new PartitionSchedulerService(config);
            scheduler.start();
            activeSchedulers.add(scheduler);
            logger.info("Started partition scheduler for entity: {}", entityName);
        }
        
        return repository;
    }
    
    private void validateParameters() {
        if (repositoryFactory == null) {
            throw new IllegalArgumentException("Repository factory is required. Use withRepositoryFactory() method.");
        }
        if (entityName == null || entityName.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity name is required");
        }
        if (dataSource == null && hikariConfig == null) {
            throw new IllegalArgumentException("Either DataSource or HikariConfig is required");
        }
    }
    
    /**
     * Stops all active schedulers and closes managed data sources. Should be called during application shutdown.
     */
    public static void stopAllSchedulers() {
        logger.info("Stopping all partition schedulers...");
        for (PartitionSchedulerService scheduler : activeSchedulers) {
            try {
                scheduler.stop();
            } catch (Exception e) {
                logger.error("Error stopping scheduler", e);
            }
        }
        activeSchedulers.clear();
        
        logger.info("Closing all managed data sources...");
        for (HikariDataSource dataSource : managedDataSources) {
            try {
                if (!dataSource.isClosed()) {
                    dataSource.close();
                }
            } catch (Exception e) {
                logger.error("Error closing data source", e);
            }
        }
        managedDataSources.clear();
    }
    
    /**
     * Registers a shutdown hook to stop all schedulers on JVM shutdown
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stopAllSchedulers();
        }));
    }
}