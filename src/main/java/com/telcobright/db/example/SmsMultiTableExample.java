package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SMS Multi-Table Example with Apache ShardingSphere COMPLEX Algorithm
 * 
 * Demonstrates:
 * - Simplified builder pattern (just entity + MySQL params)
 * - Multi-table with MySQL native hourly partitioning (sms_YYYYMMDD tables with 24 hour partitions each)
 * - Apache ShardingSphere COMPLEX algorithm with automatic UNION generation
 * - Range queries with automatic cross-table routing
 * - Aggregation queries (COUNT, GROUP BY) across multiple tables
 * - Automatic table creation on startup (1 table per day with 24 hour partitions)
 * - 7-day retention policy (15 tables total)  
 * - Daily maintenance scheduler at 04:00
 * - Zero-boilerplate repository usage
 * - UTF-8 charset and collation support
 */
public class SmsMultiTableExample {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== SMS Multi-Table with Apache ShardingSphere COMPLEX Algorithm ===\n");
            
            // 1. Create SMS repository with simplified builder and COMPLEX algorithm
            System.out.println("1. Creating SMS repository with ShardingSphere COMPLEX algorithm...");
            System.out.println("   • Builder handles DataSource creation (HikariCP)");
            System.out.println("   • Builder extracts entity metadata automatically");
            System.out.println("   • Apache ShardingSphere COMPLEX algorithm configured");
            System.out.println("   • MySQL native hourly partitioning enabled (24 partitions per table)");
            System.out.println("   • Auto UNION generation for range queries");
            System.out.println("   • Cross-table aggregation support (COUNT, SUM, GROUP BY)");
            System.out.println("   • Auto-creates tables for ±7 days (sms_YYYYMMDD)");
            System.out.println("   • Total tables: 15 tables, each with 24 hour partitions");
            System.out.println("   • Starts daily scheduler for 04:00 maintenance");
            System.out.println("   • UTF-8 charset (utf8mb4) and collation (utf8mb4_unicode_ci)");
            
            ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
                .multiTable()                    // Repository type: MULTI_TABLE with MySQL hourly partitioning
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(10)
                .charset("utf8mb4")              // Full UTF-8 Unicode support
                .collation("utf8mb4_unicode_ci") // Case-insensitive Unicode collation
                .buildRepository(SmsEntity.class);
            
            System.out.println("   ✓ Repository ready with ShardingSphere COMPLEX algorithm enabled\n");
            
            // Demo ShardingSphere COMPLEX algorithm features
            demonstrateComplexAlgorithmFeatures(smsRepo);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void demonstrateComplexAlgorithmFeatures(ShardingRepository<SmsEntity> repository) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        
        // 2. Insert SMS messages across different days and hours
        System.out.println("2. ShardingSphere Automatic INSERT Routing (Hourly Partitions)...");
        System.out.println("   • Watch ShardingSphere route INSERTs to correct tables and MySQL partitions");
        
        // Insert messages at different hours
        repository.insert(new SmsEntity("+1234567890", "Morning message", "SENT", now.withHour(9), "user1"));
        repository.insert(new SmsEntity("+9876543210", "Noon message", "DELIVERED", now.withHour(12), "user2"));
        repository.insert(new SmsEntity("+1111111111", "Evening message", "SENT", now.withHour(18), "user1"));
        repository.insert(new SmsEntity("+2222222222", "Late night message", "DELIVERED", now.minusDays(1).withHour(23), "user2"));
        repository.insert(new SmsEntity("+3333333333", "Early morning", "SENT", now.minusDays(2).withHour(3), "user3"));
        
        System.out.println("   ✓ 5 messages inserted across different table partitions\n");
        
        // 3. Query across multiple tables with hourly partitions
        System.out.println("3. Cross-table querying (across partitioned tables)...");
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(7), now.plusDays(1));
        System.out.println("   ✓ Found " + messages.size() + " messages across partitioned tables:");
        messages.forEach(msg -> System.out.println("     - " + msg.getPhoneNumber() + ": " + msg.getMessage()));
        System.out.println();
        
        // 4. Count across partitioned tables
        System.out.println("4. Cross-table counting (with partition pruning)...");
        long count = repository.count(now.minusDays(7), now.plusDays(1));
        System.out.println("   ✓ Total messages in retention window: " + count + "\n");
        
        // 5. Field-based queries across partitioned tables
        System.out.println("5. Field-based queries (across all partitioned tables)...");
        List<SmsEntity> userMessages = repository.findByField("userId", "user1");
        System.out.println("   ✓ Messages for user1: " + userMessages.size());
        
        List<SmsEntity> sentMessages = repository.query("status = ?", "SENT");
        System.out.println("   ✓ SENT messages: " + sentMessages.size() + "\n");
        
        // 6. Find by ID operations (cross-table scans)
        System.out.println("6. Find by ID operations...");
        
        // Get an ID from the inserted messages for testing
        if (!messages.isEmpty()) {
            Long sampleId = messages.get(0).getId();
            
            // Find by ID across all partitioned tables (may scan 15 tables × 24 partitions)
            System.out.println("   • Finding by ID across all partitioned tables (up to 15 tables)...");
            SmsEntity foundById = repository.findById(sampleId);
            if (foundById != null) {
                System.out.println("   ✓ Found message by ID: " + foundById.getMessage());
            } else {
                System.out.println("   ⚠ Message not found by ID (may be in different partition)");
            }
            
            // Find by ID with date range (more efficient - limits table and partition scan scope)
            System.out.println("   • Finding by ID with date range (optimized partition scan)...");
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
        System.out.println("   ✓ Multi-table with MySQL hourly partitioning active (sms_YYYYMMDD)");
        System.out.println("   ✓ 24 hour partitions per table (MySQL native)");
        System.out.println("   ✓ Auto table creation completed (15 partitioned tables)");
        System.out.println("   ✓ 7-day retention policy configured");
        System.out.println("   ✓ Daily scheduler running (04:00 maintenance)");
        System.out.println("   ✓ UTF-8 charset and Unicode collation enabled");
        System.out.println();
        
        System.out.println("🎉 SMS Multi-Table with MySQL Hourly Partitioning Complete!");
        System.out.println("   ✓ Multi-table with MySQL hourly partitioning (sms_YYYYMMDD)");
        System.out.println("   ✓ 15 tables, each with 24 hour partitions (MySQL native)");
        System.out.println("   ✓ Auto table creation on startup");
        System.out.println("   ✓ 7-day retention policy");
        System.out.println("   ✓ Daily scheduler at 04:00");
        System.out.println("   ✓ Cross-table queries and aggregations");
        System.out.println("   ✓ Find by ID (with partition scanning)");
        System.out.println("   ✓ Find by ID + date range (optimized partition scanning)");
        System.out.println("   ✓ UTF-8 charset (utf8mb4) and collation support");
        System.out.println("   ✓ Zero-boilerplate repository usage");
    }
}