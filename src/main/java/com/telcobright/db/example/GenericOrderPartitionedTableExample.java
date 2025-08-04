package com.telcobright.db.example;

import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.repository.GenericPartitionedTableRepository;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Example demonstrating generic Order repository with automatic SQL generation
 */
public class GenericOrderPartitionedTableExample {
    
    public static void main(String[] args) {
        System.out.println("=== Generic Order Partitioned Table Repository Demo ===");
        
        try {
            // Create generic repository with type safety and HikariCP connection pooling
            GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = 
                GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("ecommerce")
                    .username("root")
                    .password("password")
                    .tableName("orders")  // Optional: will use "orders" from @Table annotation if not provided
                    .partitionRetentionPeriod(365)
                    .autoManagePartitions(true)
                    .partitionAdjustmentTime(4, 0)
                    .initializePartitionsOnStart(true)
                    // HikariCP connection pool configuration for high-volume order processing
                    .maxPoolSize(30)           // Maximum 30 connections for order processing
                    .minIdleConnections(10)    // Keep 10 idle connections ready
                    .connectionTimeout(30000)  // 30 second connection timeout
                    .idleTimeout(900000)       // 15 minute idle timeout for long-running operations
                    .maxLifetime(3600000)      // 1 hour max connection lifetime
                    .leakDetectionThreshold(120000) // 2 minute leak detection for complex queries
                    .build();
            
            System.out.println(" Repository created successfully with automatic metadata parsing and HikariCP pooling");
            
            // Insert entities - SQL is automatically generated from annotations
            System.out.println("\n Inserting Order entities...");
            
            OrderEntity order1 = new OrderEntity("CUST001", "ORD-2025-001", 
                new BigDecimal("299.99"), "CONFIRMED", "CREDIT_CARD", 
                "123 Main St, City, State 12345", LocalDateTime.now(), 3);
            
            OrderEntity order2 = new OrderEntity("CUST002", "ORD-2025-002", 
                new BigDecimal("149.50"), "PENDING", "PAYPAL", 
                "456 Oak Ave, Another City, State 67890", LocalDateTime.now().minusHours(3), 1);
            
            orderRepo.insert(order1);
            orderRepo.insert(order2);
            
            System.out.println(" Inserted Order with auto-generated ID: " + order1.getId());
            System.out.println(" Inserted Order with auto-generated ID: " + order2.getId());
            
            // Find by ID (MySQL partition scan)
            System.out.println("\n Finding Order by ID...");
            OrderEntity foundOrder = orderRepo.findById(order1.getId());
            if (foundOrder != null) {
                System.out.println(" Found Order: " + foundOrder);
            } else {
                System.out.println(" Order not found");
            }
            
            // Find all orders for a customer (using date range to get recent orders)
            System.out.println("\n Finding recent orders...");
            LocalDateTime weekAgo = LocalDateTime.now().minusDays(7);
            List<OrderEntity> recentOrders = orderRepo.findByDateRange(weekAgo, LocalDateTime.now());
            System.out.println(" Found " + recentOrders.size() + " recent orders");
            
            // Find by date range (with partition pruning)
            System.out.println("\n Finding orders in last 24 hours...");
            LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
            LocalDateTime now = LocalDateTime.now();
            
            List<OrderEntity> dailyOrders = orderRepo.findByDateRange(yesterday, now);
            System.out.println(" Found " + dailyOrders.size() + " orders in date range");
            
            // Count by date range (manual count from results)
            long count = dailyOrders.size();
            System.out.println(" Total count: " + count);
            
            // Find orders before a specific date
            System.out.println("\n Finding orders before today...");
            List<OrderEntity> oldOrders = orderRepo.findBeforeDate(LocalDateTime.now().minusDays(1));
            System.out.println(" Found " + oldOrders.size() + " older orders");
            
            // Demonstrate automatic partition creation
            System.out.println("\n Testing automatic partition creation...");
            OrderEntity futureOrder = new OrderEntity("CUST003", "ORD-2025-003", 
                new BigDecimal("499.99"), "PENDING", "BANK_TRANSFER", 
                "789 Pine St, Future City, State 11111", LocalDateTime.now().plusDays(2), 5);
            
            orderRepo.insert(futureOrder);
            System.out.println(" Successfully inserted order for future date (partition auto-created)");
            
            // Graceful shutdown
            System.out.println("\n Shutting down repository...");
            orderRepo.shutdown();
            System.out.println(" Repository shutdown complete");
            
        } catch (SQLException e) {
            System.err.println(" Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println(" Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}