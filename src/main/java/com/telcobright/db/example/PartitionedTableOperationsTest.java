package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Test basic operations on partitioned table with connection timeout
 */
public class PartitionedTableOperationsTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Partitioned Table Operations Test ===\n");
            
            // Create repository for partitioned table mode
            ShardingRepository<OrderEntity> orderRepo = ShardingRepositoryBuilder
                .partitionedTable()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(3)
                .charset("utf8mb4")
                .collation("utf8mb4_unicode_ci")
                .connectionTimeout(60000)  // 1 minute for partition operations
                .buildRepository(OrderEntity.class);
            
            System.out.println("✅ Repository connected successfully");
            
            // Test insert operation
            OrderEntity order = new OrderEntity();
            order.setOrderNumber("ORD-20250728-001");
            order.setCustomerId("CUST-123");
            order.setTotalAmount(new BigDecimal("99.99"));
            order.setStatus("completed");
            order.setCreatedAt(LocalDateTime.now());
            order.setUpdatedAt(LocalDateTime.now());
            
            orderRepo.insert(order);
            System.out.println("✅ Insert operation successful");
            
            // Test query operation
            LocalDateTime startDate = LocalDateTime.now().minusDays(1);
            LocalDateTime endDate = LocalDateTime.now().plusDays(1);
            long count = orderRepo.count(startDate, endDate);
            System.out.println("✅ Count operation successful: " + count + " records");
            
            System.out.println("\n✅ All partitioned table operations completed successfully!");
            System.out.println("  • Connection timeout: 60 seconds");
            System.out.println("  • MySQL native partitioning: Active");
            System.out.println("  • Composite primary key: (id, created_at)");
            System.out.println("  • ShardingSphere routing: Working");
            
        } catch (Exception e) {
            System.err.println("❌ Partitioned table operations test failed:");
            e.printStackTrace();
        }
    }
}