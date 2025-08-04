package com.telcobright.db.example;

import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.repository.PartitionedTableRepository;
import com.telcobright.db.query.QueryDSL;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.*;

/**
 * Order Partitioned Table Strategy Example
 * Demonstrates automatic partition creation and management for Order data
 */
public class OrderPartitionedTableExample {
    
    public static void main(String[] args) {
        System.out.println("=== Order Partitioned Table Strategy Demo ===\n");
        
        try {
            demoOrderRepository();
        } catch (Exception e) {
            System.err.println("Demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void demoOrderRepository() throws SQLException {
        // Create order repository with MySQL configuration
        PartitionedTableRepository<Long> orderRepo = PartitionedTableRepository.<Long>builder()
            .host("127.0.0.1")               // MySQL host
            .port(3306)                      // MySQL port
            .database("test")                // Database name
            .username("root")                // MySQL username
            .password("123456")              // MySQL password
            .tableName("orders")             // Single partitioned table name
            .partitionRetentionPeriod(30)    // Keep 30 days of data
            .autoManagePartitions(true)      // Enable automatic cleanup
            .initializePartitionsOnStart(false) // Don't auto-create on start
            .build();
        
        System.out.println("🛒 Orders Single Partitioned Table Strategy:");
        System.out.println("- Single table 'orders' with MySQL native partitioning");
        System.out.println("- Partitions by date: p20250803, p20250804, etc.");
        System.out.println("- MySQL handles partition pruning automatically\n");
        
        // Repository configuration
        System.out.println(" Repository configured for automatic partition management:");
        System.out.println("   - Retention Period: " + orderRepo.getPartitionRetentionPeriod() + " days");
        System.out.println("   - Auto Manage: " + orderRepo.isAutoManagePartitions());
        System.out.println("   - Partitions will be created automatically during inserts\n");
        
        // Insert demo order data
        System.out.println(" Inserting demo order data...");
        insertDemoOrderData(orderRepo);
        System.out.println(" Demo order data inserted\n");
        
        // Test queries
        LocalDateTime startDate = LocalDateTime.now().minusDays(5);
        LocalDateTime endDate = LocalDateTime.now().plusDays(2);
        
        // Query 1: Basic count
        System.out.println("Query 1: Total order count");
        System.out.println("--------------------------");
        long totalOrders = orderRepo.countByDateRange(startDate, endDate);
        System.out.println("Total orders: " + totalOrders + "\n");
        
        // Query 2: Customer statistics
        System.out.println("Query 2: Top customers by spending");
        System.out.println("----------------------------------");
        List<PartitionedTableRepository.CustomerOrderStats> customerStats = 
            orderRepo.getCustomerStats(startDate, endDate, 5);
        customerStats.forEach(System.out::println);
        System.out.println();
        
        // Query 3: Daily statistics
        System.out.println("Query 3: Daily order statistics");
        System.out.println("-------------------------------");
        List<PartitionedTableRepository.DailyOrderStats> dailyStats = 
            orderRepo.getDailyStats(startDate, endDate);
        dailyStats.forEach(System.out::println);
        System.out.println();
        
        // Query 4: Custom DSL query
        System.out.println("Query 4: Custom DSL query - Orders by payment method");
        System.out.println("----------------------------------------------------");
        List<PaymentMethodStats> paymentStats = orderRepo.executeQuery(
            QueryDSL.select()
                .column("payment_method")
                .count("*", "order_count")
                .sum("total_amount", "total_revenue")
                .avg("total_amount", "avg_order_value")
                .from("orders")
                .where(w -> w
                    .dateRange("created_at", startDate, endDate)
                    .isNotNull("payment_method"))
                .groupBy("payment_method")
                .orderByDesc("total_revenue"),
            new Object[]{startDate, endDate},
            rs -> new PaymentMethodStats(
                rs.getString("payment_method"),
                rs.getLong("order_count"),
                rs.getBigDecimal("total_revenue"),
                rs.getBigDecimal("avg_order_value")
            )
        );
        
        paymentStats.forEach(System.out::println);
        System.out.println();
        
        // Query 5: Find by ID (partition scan)
        System.out.println("Query 5: Find Order by ID (partition scan)");
        System.out.println("------------------------------------------");
        // Get a sample order first to find by ID
        List<OrderEntity> sampleOrders = orderRepo.findByDateRange(
            LocalDateTime.now().minusDays(3), 
            LocalDateTime.now().minusDays(2)
        );
        
        if (!sampleOrders.isEmpty()) {
            OrderEntity sample = sampleOrders.get(0);
            System.out.println("Searching for Order with ID: " + sample.getId());
            
            OrderEntity foundById = orderRepo.findById(sample.getId());
            if (foundById != null) {
                System.out.println("Found Order: " + foundById.toString());
            } else {
                System.out.println("Order not found");
            }
            
            // Also test findAllById with custom column (using String-typed repository)
            System.out.println("Searching for Orders with customer_id: " + sample.getCustomerId());
            
            // Create a String-typed repository for customer_id searches
            PartitionedTableRepository<String> orderRepoForStrings = PartitionedTableRepository.<String>builder()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .tableName("orders")
                .partitionRetentionPeriod(30)
                .autoManagePartitions(true)
                .initializePartitionsOnStart(false)
                .build();
            
            List<OrderEntity> foundByCustomerId = orderRepoForStrings.findAllById("customer_id", sample.getCustomerId());
            System.out.println("Found " + foundByCustomerId.size() + " orders for customer: " + sample.getCustomerId());
        } else {
            System.out.println("No sample orders found for ID search test");
        }
    }
    
    private static void insertDemoOrderData(PartitionedTableRepository<Long> orderRepo) throws SQLException {
        Random rand = new Random();
        String[] customers = {"CUST001", "CUST002", "CUST003", "CUST004", "CUST005", "CUST006"};
        String[] statuses = {"PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"};
        String[] paymentMethods = {"CREDIT_CARD", "PAYPAL", "BANK_TRANSFER", "APPLE_PAY", "GOOGLE_PAY"};
        String[] addresses = {
            "123 Main St, New York, NY 10001",
            "456 Oak Ave, Los Angeles, CA 90210",
            "789 Pine Rd, Chicago, IL 60601",
            "321 Elm St, Houston, TX 77001",
            "654 Maple Dr, Phoenix, AZ 85001",
            "987 Cedar Ln, Philadelphia, PA 19101"
        };
        
        int totalInserted = 0;
        LocalDateTime baseTime = LocalDateTime.now().minusDays(4);
        
        // Generate order data across 7 days
        for (int day = 0; day < 7; day++) {
            int ordersPerDay = 8 + rand.nextInt(15); // 8-22 orders per day
            
            for (int i = 0; i < ordersPerDay; i++) {
                LocalDateTime createdAt = baseTime.plusDays(day)
                    .plusHours(rand.nextInt(24))
                    .plusMinutes(rand.nextInt(60));
                
                String status = statuses[rand.nextInt(statuses.length)];
                BigDecimal amount = new BigDecimal(25 + rand.nextInt(475)); // $25 - $500
                
                OrderEntity order = new OrderEntity(
                    customers[rand.nextInt(customers.length)],
                    "ORD" + String.format("%08d", 10000000 + totalInserted),
                    amount,
                    status,
                    paymentMethods[rand.nextInt(paymentMethods.length)],
                    addresses[rand.nextInt(addresses.length)],
                    createdAt,
                    1 + rand.nextInt(5) // 1-5 items per order
                );
                
                // Set shipping/delivery times for appropriate statuses
                if ("SHIPPED".equals(status) || "DELIVERED".equals(status)) {
                    order.setShippedAt(createdAt.plusHours(12 + rand.nextInt(36)));
                }
                if ("DELIVERED".equals(status)) {
                    order.setDeliveredAt(createdAt.plusDays(1 + rand.nextInt(3)));
                }
                
                orderRepo.insert(order);
                totalInserted++;
            }
        }
        
        System.out.println("   Inserted " + totalInserted + " orders across 7 days");
    }
    
    // Helper class for custom queries
    static class PaymentMethodStats {
        final String paymentMethod;
        final long orderCount;
        final BigDecimal totalRevenue;
        final BigDecimal avgOrderValue;
        
        PaymentMethodStats(String paymentMethod, long orderCount, 
                          BigDecimal totalRevenue, BigDecimal avgOrderValue) {
            this.paymentMethod = paymentMethod;
            this.orderCount = orderCount;
            this.totalRevenue = totalRevenue;
            this.avgOrderValue = avgOrderValue;
        }
        
        @Override
        public String toString() {
            return String.format("Payment: %s | Orders: %d | Revenue: $%s | Avg: $%s",
                paymentMethod, orderCount, totalRevenue, avgOrderValue);
        }
    }
    
}