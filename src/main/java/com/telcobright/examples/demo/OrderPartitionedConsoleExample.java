package com.telcobright.examples.demo;

import com.telcobright.examples.entity.OrderEntity;
import com.telcobright.db.repository.GenericPartitionedTableRepository;
import com.telcobright.db.monitoring.MonitoringConfig;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating Order partitioned table repository with console monitoring
 * 
 * Features shown:
 * - Single partitioned table with MySQL native partitioning
 * - Console monitoring output with formatted metrics
 * - E-commerce order management
 * - Automatic partition creation and maintenance
 * - 7-day default retention period with daily partitions
 */
public class OrderPartitionedConsoleExample {
    
    private static OrderEntity createDeliveredOrder(String customerId, String orderNumber, BigDecimal totalAmount, 
                                                   String status, String paymentMethod, String shippingAddress,
                                                   LocalDateTime createdAt, LocalDateTime shippedAt, 
                                                   LocalDateTime deliveredAt, Integer itemCount) {
        OrderEntity order = new OrderEntity(customerId, orderNumber, totalAmount, status, paymentMethod, 
                                           shippingAddress, createdAt, itemCount);
        order.setShippedAt(shippedAt);
        order.setDeliveredAt(deliveredAt);
        return order;
    }
    
    private static OrderEntity createShippedOrder(String customerId, String orderNumber, BigDecimal totalAmount, 
                                                 String status, String paymentMethod, String shippingAddress,
                                                 LocalDateTime createdAt, LocalDateTime shippedAt, Integer itemCount) {
        OrderEntity order = new OrderEntity(customerId, orderNumber, totalAmount, status, paymentMethod, 
                                           shippingAddress, createdAt, itemCount);
        order.setShippedAt(shippedAt);
        return order;
    }
    
    public static void main(String[] args) {
        System.out.println("=== Order Partitioned Table Repository with Console Monitoring ===\n");
        
        // Configure console monitoring
        MonitoringConfig consoleMonitoring = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.FORMATTED_TEXT)
            .reportingIntervalSeconds(60)  // Report every minute for demo
            .serviceName("ecommerce-service")
            .instanceId("order-server-01")
            .build();
        
        // Create Order repository with console monitoring
        GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = null;
        try {
            orderRepo = GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                .host("127.0.0.1")
                .port(3306)
                .database("ecommerce")
                .username("root")
                .password("123456")
                .tableName("orders")
                .partitionRetentionPeriod(7)  // 7 days retention (15 partitions total)
                .autoManagePartitions(true)   // Enable automatic maintenance
                .partitionAdjustmentTime(4, 0) // Maintenance at 04:00 AM
                .initializePartitionsOnStart(true)
                .monitoring(consoleMonitoring) // Enable console monitoring
                .build();
            
            System.out.println("✅ Order Repository created successfully");
            System.out.println("   - Table Structure: Single table with MySQL native partitioning");
            System.out.println("   - Total Partitions: 15 daily partitions (p20250731 to p20250814)");
            System.out.println("   - Partitioning: RANGE partitioning by TO_DAYS(created_at)");
            System.out.println("   - Monitoring: Console output every 60 seconds\n");
            
            // Insert sample orders
            System.out.println("🛒 Inserting e-commerce orders...");
            
            List<OrderEntity> sampleOrders = Arrays.asList(
                new OrderEntity("CUST001", "ORD-2025-001", new BigDecimal("299.99"), 
                               "CONFIRMED", "CREDIT_CARD", "123 Main St, City", LocalDateTime.now(), 3),
                new OrderEntity("CUST002", "ORD-2025-002", new BigDecimal("159.50"), 
                               "SHIPPED", "PAYPAL", "456 Oak Ave, Town", LocalDateTime.now().minusHours(8), 2),
                createDeliveredOrder("CUST001", "ORD-2025-003", new BigDecimal("89.99"), 
                               "DELIVERED", "CREDIT_CARD", "123 Main St, City", 
                               LocalDateTime.now().minusDays(1), LocalDateTime.now().minusDays(1).plusHours(6), 
                               LocalDateTime.now().minusDays(1).plusHours(12), 1),
                new OrderEntity("CUST003", "ORD-2025-004", new BigDecimal("524.75"), 
                               "PROCESSING", "BANK_TRANSFER", "789 Pine Rd, Village", 
                               LocalDateTime.now().minusDays(2), 5)
            );
            
            // Insert orders individually to show auto-generation
            for (int i = 0; i < sampleOrders.size(); i++) {
                orderRepo.insert(sampleOrders.get(i));
                System.out.printf("   ✓ Inserted Order %d with auto-generated ID\n", i + 1);
            }
            
            // Insert batch of orders from different time periods
            List<OrderEntity> batchOrders = Arrays.asList(
                new OrderEntity("CUST004", "ORD-2025-005", new BigDecimal("199.99"), 
                               "CANCELLED", "CREDIT_CARD", "321 Elm St, Suburb", 
                               LocalDateTime.now().minusDays(3), 2),
                createShippedOrder("CUST002", "ORD-2025-006", new BigDecimal("449.99"), 
                               "SHIPPED", "DEBIT_CARD", "456 Oak Ave, Town", 
                               LocalDateTime.now().minusDays(1).minusHours(4), LocalDateTime.now().minusHours(2), 4),
                new OrderEntity("CUST005", "ORD-2025-007", new BigDecimal("75.25"), 
                               "CONFIRMED", "APPLE_PAY", "654 Birch Ln, City", 
                               LocalDateTime.now().minusHours(3), 1)
            );
            
            orderRepo.insertMultiple(batchOrders);
            System.out.println("   ✓ Inserted batch of 3 orders\n");
            
            // Query operations to demonstrate partitioned table functionality
            System.out.println("🔍 Performing query operations...");
            
            // Find orders from last 48 hours (demonstrates partition pruning)
            List<OrderEntity> recentOrders = orderRepo.findAllByDateRange(
                LocalDateTime.now().minusDays(2), 
                LocalDateTime.now()
            );
            System.out.printf("   Found %d orders in last 48 hours\n", recentOrders.size());
            
            // Calculate total revenue from recent orders
            BigDecimal totalRevenue = recentOrders.stream()
                .map(OrderEntity::getTotalAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
            System.out.printf("   Total revenue in last 48 hours: $%.2f\n", totalRevenue);
            
            // Find a specific order by ID (demonstrates MySQL partition scanning)
            if (!recentOrders.isEmpty()) {
                OrderEntity firstOrder = recentOrders.get(0);
                OrderEntity foundOrder = orderRepo.findById(firstOrder.getId());
                if (foundOrder != null) {
                    System.out.printf("   ✓ Found order by ID: %d (%s - $%.2f)\n", 
                                    foundOrder.getId(), foundOrder.getStatus(), foundOrder.getTotalAmount());
                }
            }
            
            // Update an order status with date range constraint
            if (!recentOrders.isEmpty()) {
                OrderEntity orderToUpdate = recentOrders.stream()
                    .filter(o -> "PROCESSING".equals(o.getStatus()) || "CONFIRMED".equals(o.getStatus()))
                    .findFirst().orElse(recentOrders.get(0));
                
                OrderEntity updateData = new OrderEntity();
                updateData.setStatus("SHIPPED");
                updateData.setShippedAt(LocalDateTime.now());
                
                orderRepo.updateByIdAndDateRange(orderToUpdate.getId(), updateData, 
                    LocalDateTime.now().minusDays(7), LocalDateTime.now());
                System.out.printf("   ✓ Updated order ID %d to SHIPPED status\n", orderToUpdate.getId());
            }
            
            // Find orders before a specific date (demonstrates partition elimination)
            List<OrderEntity> oldOrders = orderRepo.findAllBeforeDate(LocalDateTime.now().minusDays(2));
            System.out.printf("   Found %d orders older than 2 days\n", oldOrders.size());
            
            System.out.println("\n⏱️  Waiting for monitoring output (60 seconds)...");
            System.out.println("    Console monitoring will automatically display metrics");
            System.out.println("    Watch for partition-specific metrics and maintenance info");
            
            // Keep the application running to see monitoring output
            Thread.sleep(70000); // 70 seconds to see at least one monitoring report
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (orderRepo != null) {
                System.out.println("\n🛑 Shutting down Order repository...");
                orderRepo.shutdown();
                System.out.println("   ✓ Repository shut down gracefully");
            }
        }
        
        System.out.println("\n=== Order Partitioned Table Example Complete ===");
    }
}