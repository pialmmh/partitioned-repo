package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SMS Multi-Table Example with Simplified Builder
 * 
 * Demonstrates:
 * - Simplified builder pattern (just entity + MySQL params)
 * - Multi-table sharding (sms_YYYYMMDD tables)
 * - Automatic table creation on startup
 * - 7-day retention policy  
 * - Daily maintenance scheduler at 04:00
 * - Zero-boilerplate repository usage
 */
public class SmsMultiTableExample {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== SMS Multi-Table Sharding with Simplified Builder ===\n");
            
            // 1. Create SMS repository with simplified builder
            // Just provide entity class + MySQL parameters - builder handles everything else
            System.out.println("1. Creating SMS repository with simplified builder...");
            System.out.println("   • Builder handles DataSource creation (HikariCP)");
            System.out.println("   • Builder extracts entity metadata automatically");
            System.out.println("   • Builder configures ShardingSphere with sharding rules");
            System.out.println("   • Auto-creates tables for ±7 days (sms_YYYYMMDD)");
            System.out.println("   • Starts daily scheduler for 04:00 maintenance");
            
            ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
                .multiTable()                    // Repository type: MULTI_TABLE
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(10)
                .buildRepository(SmsEntity.class);
            
            System.out.println("   ✓ Repository ready with auto-management enabled\n");
            
            // Demo operations
            demonstrateSmsOperations(smsRepo);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void demonstrateSmsOperations(ShardingRepository<SmsEntity> repository) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        
        // 2. Insert SMS messages across different days
        System.out.println("2. Inserting SMS messages...");
        System.out.println("   • Messages will be auto-routed to correct daily tables");
        
        repository.insert(new SmsEntity("+1234567890", "Today's message", "SENT", now, "user1"));
        repository.insert(new SmsEntity("+9876543210", "Another today message", "DELIVERED", now, "user2"));
        repository.insert(new SmsEntity("+1111111111", "Yesterday's message", "SENT", now.minusDays(1), "user1"));
        repository.insert(new SmsEntity("+2222222222", "Two days ago", "DELIVERED", now.minusDays(2), "user2"));
        
        System.out.println("   ✓ 4 messages inserted across multiple tables\n");
        
        // 3. Query across multiple tables
        System.out.println("3. Cross-table querying...");
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(7), now.plusDays(1));
        System.out.println("   ✓ Found " + messages.size() + " messages across all tables:");
        messages.forEach(msg -> System.out.println("     - " + msg.getPhoneNumber() + ": " + msg.getMessage()));
        System.out.println();
        
        // 4. Count across tables
        System.out.println("4. Cross-table counting...");
        long count = repository.count(now.minusDays(7), now.plusDays(1));
        System.out.println("   ✓ Total messages in retention window: " + count + "\n");
        
        // 5. Field-based queries
        System.out.println("5. Field-based queries...");
        List<SmsEntity> userMessages = repository.findByField("userId", "user1");
        System.out.println("   ✓ Messages for user1: " + userMessages.size());
        
        List<SmsEntity> sentMessages = repository.query("status = ?", "SENT");
        System.out.println("   ✓ SENT messages: " + sentMessages.size() + "\n");
        
        // 6. Find by ID operations (cross-table scans)
        System.out.println("6. Find by ID operations...");
        
        // Get an ID from the inserted messages for testing
        if (!messages.isEmpty()) {
            Long sampleId = messages.get(0).getId();
            
            // Find by ID across all retention tables (may scan multiple tables)
            System.out.println("   • Finding by ID across all tables (full retention window)...");
            SmsEntity foundById = repository.findById(sampleId);
            if (foundById != null) {
                System.out.println("   ✓ Found message by ID: " + foundById.getMessage());
            } else {
                System.out.println("   ⚠ Message not found by ID (may be in different table)");
            }
            
            // Find by ID with date range (more efficient - limits table scan scope)
            System.out.println("   • Finding by ID with date range (optimized scan)...");
            SmsEntity foundByIdAndDate = repository.findByIdAndDateRange(sampleId, now.minusDays(1), now.plusDays(1));
            if (foundByIdAndDate != null) {
                System.out.println("   ✓ Found message by ID+date: " + foundByIdAndDate.getMessage());
            } else {
                System.out.println("   ⚠ Message not found in specified date range");
            }
        } else {
            System.out.println("   ⚠ No messages available for ID lookup test");
        }
        System.out.println();
        
        // 7. Show current status
        System.out.println("7. Repository status:");
        System.out.println("   ✓ Multi-table sharding active (sms_YYYYMMDD)");
        System.out.println("   ✓ Auto table creation completed");
        System.out.println("   ✓ 7-day retention policy configured");
        System.out.println("   ✓ Daily scheduler running (04:00 maintenance)");
        System.out.println();
        
        System.out.println("🎉 SMS Multi-Table Sharding Complete!");
        System.out.println("   ✓ Multi-table sharding (sms_YYYYMMDD)");
        System.out.println("   ✓ Auto table creation on startup");
        System.out.println("   ✓ 7-day retention policy");
        System.out.println("   ✓ Daily scheduler at 04:00");
        System.out.println("   ✓ Cross-table queries and aggregations");
        System.out.println("   ✓ Find by ID (with cross-table scanning)");
        System.out.println("   ✓ Find by ID + date range (optimized scanning)");
        System.out.println("   ✓ Zero-boilerplate repository usage");
    }
}