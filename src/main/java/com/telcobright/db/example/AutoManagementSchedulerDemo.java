package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.repository.MultiTableRepository;
import com.telcobright.db.repository.PartitionedTableRepository;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.math.BigDecimal;

/**
 * Demonstrates the automatic scheduled partition management functionality
 * Shows how to configure daily schedulers for both repository types
 */
public class AutoManagementSchedulerDemo {
    
    public static void main(String[] args) {
        System.out.println("=== Automatic Partition Management Scheduler Demo ===\n");
        
        try {
            demonstrateSchedulers();
        } catch (Exception e) {
            System.err.println("Demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void demonstrateSchedulers() throws SQLException, InterruptedException {
        
        // Demo 1: MultiTableRepository with custom adjustment time
        System.out.println("Creating MultiTableRepository with scheduled maintenance...");
        MultiTableRepository<Long> smsRepo = MultiTableRepository.<Long>builder()
            .host("127.0.0.1")
            .port(3306)
            .database("test")
            .username("root")
            .password("123456")
            .tablePrefix("sms")
            .partitionRetentionPeriod(7)           // Keep 7 days of data
            .autoManagePartitions(true)            // Enable auto-management
            .partitionAdjustmentTime(2, 30)        // Daily maintenance at 02:30
            .initializePartitionsOnStart(false)
            .build();
        
        System.out.println(" SMS Repository created with scheduler");
        System.out.println("   - Retention period: " + smsRepo.getPartitionRetentionPeriod() + " days");
        System.out.println("   - Daily maintenance at: " + smsRepo.getPartitionAdjustmentTime());
        System.out.println("   - Auto-manage enabled: " + smsRepo.isAutoManagePartitions());
        System.out.println();
        
        // Demo 2: PartitionedTableRepository with default adjustment time
        System.out.println(" Creating PartitionedTableRepository with scheduled maintenance...");
        PartitionedTableRepository<Long> orderRepo = PartitionedTableRepository.<Long>builder()
            .host("127.0.0.1")
            .port(3306)
            .database("test")
            .username("root")
            .password("123456")
            .tableName("orders")
            .partitionRetentionPeriod(30)          // Keep 30 days of data
            .autoManagePartitions(true)            // Enable auto-management
            .partitionAdjustmentTime(LocalTime.of(4, 0))  // Daily maintenance at 04:00 (default)
            .initializePartitionsOnStart(false)
            .build();
        
        System.out.println(" Order Repository created with scheduler");
        System.out.println("   - Retention period: " + orderRepo.getPartitionRetentionPeriod() + " days");
        System.out.println("   - Daily maintenance at: " + orderRepo.getPartitionAdjustmentTime());
        System.out.println("   - Auto-manage enabled: " + orderRepo.isAutoManagePartitions());
        System.out.println();
        
        // Demo 3: Show what happens during regular operations
        System.out.println(" Testing insert operations (triggers immediate maintenance)...");
        
        // Insert some test data
        SmsEntity sms = new SmsEntity("user001", "+1234567890", 
            "Scheduler demo message", "SENT", LocalDateTime.now(), 
            new BigDecimal("0.05"), "demo-provider");
        smsRepo.insert(sms);
        System.out.println("    SMS inserted (triggered table maintenance)");
        
        OrderEntity order = new OrderEntity("CUST001", "ORD-SCHEDULER-001", 
            new BigDecimal("99.99"), "CONFIRMED", "CREDIT_CARD", 
            "123 Demo St", LocalDateTime.now(), 2);
        orderRepo.insert(order);
        System.out.println("    Order inserted (triggered partition maintenance)");
        System.out.println();
        
        // Demo 4: Manual maintenance trigger
        System.out.println(" Triggering manual maintenance...");
        smsRepo.cleanupOldTables();
        orderRepo.cleanupOldPartitions();
        System.out.println("    Manual maintenance completed");
        System.out.println();
        
        // Demo 5: Show scheduler behavior
        System.out.println("⏰ Schedulers are now running in background:");
        System.out.println("   - SMS tables: Daily cleanup at " + smsRepo.getPartitionAdjustmentTime());
        System.out.println("   - Order partitions: Daily cleanup at " + orderRepo.getPartitionAdjustmentTime());
        System.out.println();
        
        System.out.println(" Current behavior:");
        System.out.println("    On insert: Immediate maintenance (create future tables/partitions, cleanup old ones)");
        System.out.println("    Daily schedule: Background maintenance at configured time");
        System.out.println("    Retention range: {today - retentionDays} to {today + retentionDays}");
        System.out.println("    Graceful error handling with detailed logging");
        System.out.println();
        
        // Let the demo run for a bit to show scheduler behavior
        System.out.println(" Demo running... Schedulers active in background.");
        System.out.println(" In production, schedulers will run daily at configured times.");
        System.out.println(" Check logs for scheduler status and maintenance operations.");
        
        // Keep the demo alive for a few seconds to show scheduler initialization
        Thread.sleep(5000);
        
        // Cleanup
        System.out.println();
        System.out.println("🛑 Shutting down schedulers...");
        smsRepo.shutdown();
        orderRepo.shutdown();
        System.out.println(" Schedulers stopped gracefully");
        
        System.out.println();
        System.out.println(" Scheduler demo completed successfully!");
        System.out.println(" Key features implemented:");
        System.out.println("   - Daily scheduled maintenance at configurable time");
        System.out.println("   - Automatic table/partition creation and cleanup");
        System.out.println("   - Proper retention period management");
        System.out.println("   - Graceful scheduler shutdown");
        System.out.println("   - Background thread safety");
    }
}