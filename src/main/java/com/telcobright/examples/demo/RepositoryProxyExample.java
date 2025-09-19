package com.telcobright.examples.demo;

import com.telcobright.api.RepositoryProxy;
import com.telcobright.api.ShardingRepository;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.examples.entity.OrderEntity;
import com.telcobright.examples.entity.SmsEntity;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Example demonstrating the use of RepositoryProxy for unified API access
 */
public class RepositoryProxyExample {
    
    public static void main(String[] args) throws SQLException {
        // Example 1: SMS Repository via Split-Verse Proxy
        ShardConfig smsShardConfig = ShardConfig.builder()
            .shardId("sms-primary")
            .host("127.0.0.1")
            .port(3306)
            .database("test")
            .username("root")
            .password("123456")
            .connectionPoolSize(10)
            .enabled(true)
            .build();
        
        SplitVerseRepository<SmsEntity, LocalDateTime> smsRepo =
            SplitVerseRepository.builder()
                .withSingleShard(smsShardConfig)
                .withEntityClass(SmsEntity.class)
                .build();
        
        // SplitVerseRepository implements ShardingRepository, so can be used directly
        ShardingRepository<SmsEntity, LocalDateTime> smsProxy = smsRepo;
        
        // Example 2: Order Repository via Split-Verse Proxy
        ShardConfig orderShardConfig = ShardConfig.builder()
            .shardId("order-primary")
            .host("127.0.0.1")
            .port(3306)
            .database("test")
            .username("root")
            .password("123456")
            .connectionPoolSize(10)
            .enabled(true)
            .build();
        
        SplitVerseRepository<OrderEntity, LocalDateTime> orderRepo = 
            SplitVerseRepository.builder()
                .withSingleShard(orderShardConfig)
                .withEntityClass(OrderEntity.class)
                .build();
        
        // SplitVerseRepository implements ShardingRepository, so can be used directly
        ShardingRepository<OrderEntity, LocalDateTime> orderProxy = orderRepo;
        
        // Both repositories can now be used through the same ShardingRepository interface
        demonstrateCommonOperations(smsProxy, "SMS Repository");
        demonstrateCommonOperations(orderProxy, "Order Repository");
        
        // With Split-Verse, all repositories are sharding-aware by default
        System.out.println("SMS Repository: Split-Verse sharding enabled");
        System.out.println("Order Repository: Split-Verse sharding enabled");
        
        // Split-Verse handles all sharding internally - no need to access underlying implementation
        
        // Cleanup
        smsProxy.shutdown();
        orderProxy.shutdown();
    }
    
    /**
     * Demonstrate common operations that work with any ShardingRepository
     */
    private static <T extends com.telcobright.core.entity.ShardingEntity> 
            void demonstrateCommonOperations(ShardingRepository<T, LocalDateTime> repo, String repoName) {
        
        try {
            System.out.println("\n=== " + repoName + " ===");
            
            // Find entities in date range
            LocalDateTime startDate = LocalDateTime.now().minusDays(7);
            LocalDateTime endDate = LocalDateTime.now();
            List<T> entities = repo.findAllByDateRange(startDate, endDate);
            System.out.println("Found " + entities.size() + " entities in last 7 days");
            
            // Cursor-based iteration
            String cursor = null;
            T nextEntity = repo.findOneByIdGreaterThan(cursor);
            if (nextEntity != null) {
                System.out.println("Found next entity using cursor-based iteration");
            }
            
            // Batch processing
            List<T> batch = repo.findBatchByIdGreaterThan(cursor, 10);
            System.out.println("Retrieved batch of " + batch.size() + " entities");
            
        } catch (SQLException e) {
            System.err.println("Error accessing " + repoName + ": " + e.getMessage());
        }
    }
}