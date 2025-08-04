package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.repository.MultiTableRepository;
import com.telcobright.db.query.QueryDSL;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.*;

/**
 * SMS Multi-Table Strategy Example
 * Demonstrates automatic table creation and management for SMS data
 */
public class SmsMultiTableExample {
    
    public static void main(String[] args) {
        System.out.println("=== SMS Multi-Table Strategy Demo ===\n");
        
        try {
            demoSmsRepository();
        } catch (Exception e) {
            System.err.println("Demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void demoSmsRepository() throws SQLException {
        // Create SMS repository with MySQL configuration
        MultiTableRepository<Long> smsRepo = MultiTableRepository.<Long>builder()
            .host("127.0.0.1")               // MySQL host
            .port(3306)                      // MySQL port
            .database("test")                // Database name
            .username("root")                // MySQL username
            .password("123456")              // MySQL password
            .tablePrefix("sms")              // Table prefix for sms_YYYYMMDD tables
            .partitionRetentionPeriod(7)     // Keep 7 days of data
            .autoManagePartitions(true)      // Enable automatic cleanup
            .initializePartitionsOnStart(false) // Don't auto-create on start
            .build();
        
        System.out.println("SMS Multi-Table Strategy:");
        System.out.println("- Creates separate tables: sms_20250803, sms_20250804, etc.");
        System.out.println("- Uses UNION ALL for cross-table queries");
        System.out.println("- Each table can be optimized independently\n");
        
        // Repository configuration
        System.out.println("Repository configured for automatic table management:");
        System.out.println("   - Retention Period: " + smsRepo.getPartitionRetentionPeriod() + " days");
        System.out.println("   - Auto Manage: " + smsRepo.isAutoManagePartitions());
        System.out.println("   - Tables will be created automatically during inserts\n");
        
        // Insert demo SMS data
        System.out.println("Inserting demo SMS data...");
        insertDemoSmsData(smsRepo);
        System.out.println("Demo SMS data inserted\n");
        
        // Test queries
        LocalDateTime startDate = LocalDateTime.now().minusDays(5);
        LocalDateTime endDate = LocalDateTime.now().plusDays(2);
        
        // Query 1: Basic count
        System.out.println("Query 1: Total SMS count");
        System.out.println("------------------------");
        long totalSms = smsRepo.countByDateRange(startDate, endDate);
        System.out.println("Total SMS messages: " + totalSms + "\n");
        
        // Query 2: User statistics with 2-level aggregation
        System.out.println("Query 2: Top users by message count (2-level aggregation)");
        System.out.println("---------------------------------------------------------");
        List<MultiTableRepository.UserSmsStats> userStats = smsRepo.getUserStats(startDate, endDate, 5);
        userStats.forEach(System.out::println);
        System.out.println();
        
        // Query 3: Hourly statistics
        System.out.println("Query 3: Recent hourly SMS statistics");
        System.out.println("------------------------------------");
        List<MultiTableRepository.HourlyStats> hourlyStats = smsRepo.getHourlyStats(
            LocalDateTime.now().minusDays(1), LocalDateTime.now());
        hourlyStats.stream().limit(5).forEach(System.out::println);
        System.out.println();
        
        // Query 4: Custom DSL query
        System.out.println("Query 4: Custom DSL query - SMS by status");
        System.out.println("-----------------------------------------");
        List<SmsStatusStats> statusStats = smsRepo.executePartitionedQuery(
            QueryDSL.select()
                .column("status")
                .count("*", "count")
                .sum("cost", "total_cost")
                .avg("LENGTH(message)", "avg_length")
                .from("sms")
                .where(w -> w
                    .dateRange("created_at", startDate, endDate)
                    .in("status", "SENT", "DELIVERED", "FAILED"))
                .groupBy("status")
                .orderByDesc("count"),
            startDate, endDate,
            new Object[]{startDate, endDate},
            rs -> new SmsStatusStats(
                rs.getString("status"),
                rs.getLong("count"),
                rs.getBigDecimal("total_cost"),
                rs.getDouble("avg_length")
            )
        );
        
        statusStats.forEach(System.out::println);
        System.out.println();
        
        // Query 5: Find by ID (full table scan)
        System.out.println("Query 5: Find SMS by ID (full table scan)");
        System.out.println("---------------------------------------");
        // Get a sample SMS first to find by ID
        List<SmsEntity> sampleSms = smsRepo.findByDateRange(
            LocalDateTime.now().minusDays(2), 
            LocalDateTime.now().minusDays(1)
        );
        
        if (!sampleSms.isEmpty()) {
            SmsEntity sample = sampleSms.get(0);
            System.out.println("Searching for SMS with ID: " + sample.getId());
            
            SmsEntity foundById = smsRepo.findById(sample.getId());
            if (foundById != null) {
                System.out.println("Found SMS: " + foundById.toString());
            } else {
                System.out.println("SMS not found");
            }
            
            // Also test findAllById with custom column (using String-typed repository)
            System.out.println("Searching for SMS with user_id: " + sample.getUserId());
            
            // Create a String-typed repository for user_id searches
            MultiTableRepository<String> smsRepoForStrings = MultiTableRepository.<String>builder()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .tablePrefix("sms")
                .partitionRetentionPeriod(7)
                .autoManagePartitions(true)
                .initializePartitionsOnStart(false)
                .build();
            
            List<SmsEntity> foundByUserId = smsRepoForStrings.findAllById("user_id", sample.getUserId());
            System.out.println("Found " + foundByUserId.size() + " SMS messages for user: " + sample.getUserId());
        } else {
            System.out.println("No sample SMS found for ID search test");
        }
    }
    
    private static void insertDemoSmsData(MultiTableRepository<Long> smsRepo) throws SQLException {
        Random rand = new Random();
        String[] users = {"user001", "user002", "user003", "user004", "user005"};
        String[] statuses = {"SENT", "DELIVERED", "FAILED", "PENDING"};
        String[] providers = {"Twilio", "MessageBird", "AWS SNS", "Vonage"};
        String[] messages = {
            "Your order has been confirmed and will be shipped soon.",
            "Welcome to our service! Thanks for signing up.",
            "Your verification code is: 123456",
            "Reminder: Your appointment is tomorrow at 3 PM.",
            "Flash sale! 50% off all items. Limited time offer!",
            "Your package has been delivered successfully.",
            "Payment received. Thank you for your purchase.",
            "Account balance low. Please top up your account."
        };
        
        int totalInserted = 0;
        LocalDateTime baseTime = LocalDateTime.now().minusDays(4);
        
        // Generate SMS data across 7 days
        for (int day = 0; day < 7; day++) {
            int messagesPerDay = 15 + rand.nextInt(20); // 15-35 messages per day
            
            for (int i = 0; i < messagesPerDay; i++) {
                LocalDateTime createdAt = baseTime.plusDays(day)
                    .plusHours(rand.nextInt(24))
                    .plusMinutes(rand.nextInt(60));
                
                String status = statuses[rand.nextInt(statuses.length)];
                String message = messages[rand.nextInt(messages.length)];
                
                SmsEntity sms = new SmsEntity(
                    users[rand.nextInt(users.length)],
                    "+1555" + String.format("%07d", 1000000 + rand.nextInt(9000000)),
                    message,
                    status,
                    createdAt,
                    new BigDecimal("0.0" + (5 + rand.nextInt(15))), // $0.05 - $0.19
                    providers[rand.nextInt(providers.length)]
                );
                
                // Set delivered time for delivered messages
                if ("DELIVERED".equals(status)) {
                    sms.setDeliveredAt(createdAt.plusSeconds(5 + rand.nextInt(55)));
                }
                
                smsRepo.insert(sms);
                totalInserted++;
            }
        }
        
        System.out.println("   Inserted " + totalInserted + " SMS messages across 7 days");
    }
    
    // Helper class for custom queries
    static class SmsStatusStats {
        final String status;
        final long count;
        final BigDecimal totalCost;
        final double avgLength;
        
        SmsStatusStats(String status, long count, BigDecimal totalCost, double avgLength) {
            this.status = status;
            this.count = count;
            this.totalCost = totalCost;
            this.avgLength = avgLength;
        }
        
        @Override
        public String toString() {
            return String.format("Status: %s | Count: %d | Cost: $%s | Avg Length: %.1f chars",
                status, count, totalCost, avgLength);
        }
    }
    
}