package com.telcobright.examples.demo;

import com.telcobright.api.RepositoryProxy;
import com.telcobright.api.ShardingRepository;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
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
        // Example 1: Multi-Table Repository via Proxy
        GenericMultiTableRepository<SmsEntity> smsRepo = 
            GenericMultiTableRepository.<SmsEntity>builder(SmsEntity.class)
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .tablePrefix("sms")
                .partitionRetentionPeriod(7)
                .build();
        
        RepositoryProxy<SmsEntity> smsProxy = RepositoryProxy.forMultiTable(smsRepo);
        
        // Example 2: Partitioned Table Repository via Proxy
        GenericPartitionedTableRepository<OrderEntity> orderRepo = 
            GenericPartitionedTableRepository.<OrderEntity>builder(OrderEntity.class)
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .tableName("orders")
                .partitionRetentionPeriod(30)
                .build();
        
        RepositoryProxy<OrderEntity> orderProxy = RepositoryProxy.forPartitionedTable(orderRepo);
        
        // Both repositories can now be used through the same ShardingRepository interface
        demonstrateCommonOperations(smsProxy, "SMS Repository");
        demonstrateCommonOperations(orderProxy, "Order Repository");
        
        // Check repository type if needed
        System.out.println("SMS Repo Type: " + smsProxy.getType());
        System.out.println("Order Repo Type: " + orderProxy.getType());
        
        // Access underlying implementation if needed (use with caution)
        if (smsProxy.getType() == RepositoryProxy.RepositoryType.MULTI_TABLE) {
            GenericMultiTableRepository<SmsEntity> underlying = smsProxy.getDelegate();
            // Can access multi-table specific methods if needed
        }
        
        // Cleanup
        smsProxy.shutdown();
        orderProxy.shutdown();
    }
    
    /**
     * Demonstrate common operations that work with any ShardingRepository
     */
    private static <T extends com.telcobright.core.entity.ShardingEntity> 
            void demonstrateCommonOperations(ShardingRepository<T> repo, String repoName) {
        
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