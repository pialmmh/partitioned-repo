package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Example demonstrating generic SMS repository with automatic SQL generation
 */
public class GenericSmsMultiTableExample {
    
    public static void main(String[] args) {
        System.out.println("=== Generic SMS Multi-Table Repository Demo ===");
        
        try {
            // Create generic repository with type safety and HikariCP connection pooling
            GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
                GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("messaging")
                    .username("root")
                    .password("password")
                    .tablePrefix("sms")  // Optional: will use "sms" from @Table annotation if not provided
                    .partitionRetentionPeriod(30)
                    .autoManagePartitions(true)
                    .partitionAdjustmentTime(4, 0)
                    .initializePartitionsOnStart(true)
                    // HikariCP connection pool configuration
                    .maxPoolSize(25)           // Maximum 25 connections in pool
                    .minIdleConnections(5)     // Minimum 5 idle connections
                    .connectionTimeout(30000)  // 30 second connection timeout
                    .idleTimeout(600000)       // 10 minute idle timeout
                    .maxLifetime(1800000)      // 30 minute max connection lifetime
                    .leakDetectionThreshold(60000) // 1 minute leak detection
                    .build();
            
            System.out.println("✅ Repository created successfully with automatic metadata parsing and HikariCP pooling");
            
            // Insert entities - SQL is automatically generated from annotations
            System.out.println("\n📝 Inserting SMS entities...");
            
            SmsEntity sms1 = new SmsEntity("user123", "+1234567890", 
                "Hello World!", "SENT", LocalDateTime.now(), 
                new BigDecimal("0.05"), "twilio");
            
            SmsEntity sms2 = new SmsEntity("user456", "+0987654321", 
                "Test message", "PENDING", LocalDateTime.now().minusHours(2), 
                new BigDecimal("0.03"), "aws-sns");
            
            smsRepo.insert(sms1);
            smsRepo.insert(sms2);
            
            System.out.println("✅ Inserted SMS with auto-generated ID: " + sms1.getId());
            System.out.println("✅ Inserted SMS with auto-generated ID: " + sms2.getId());
            
            // Find by ID across all tables
            System.out.println("\n🔍 Finding SMS by ID...");
            SmsEntity foundSms = smsRepo.findById(sms1.getId());
            if (foundSms != null) {
                System.out.println("✅ Found SMS: " + foundSms);
            } else {
                System.out.println("❌ SMS not found");
            }
            
            // Find all SMS for a user
            System.out.println("\n👤 Finding all SMS for user123...");
            List<SmsEntity> userSms = smsRepo.findAllById("user_id", "user123");
            System.out.println("✅ Found " + userSms.size() + " SMS messages for user123");
            
            // Find by date range
            System.out.println("\n📅 Finding SMS in last 24 hours...");
            LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
            LocalDateTime now = LocalDateTime.now();
            
            List<SmsEntity> recentSms = smsRepo.findByDateRange(yesterday, now);
            System.out.println("✅ Found " + recentSms.size() + " SMS messages in date range");
            
            // Count by date range
            long count = smsRepo.countByDateRange(yesterday, now);
            System.out.println("✅ Total count: " + count);
            
            // Demonstrate automatic table creation
            System.out.println("\n🏗️ Testing automatic table creation...");
            SmsEntity futureMessage = new SmsEntity("user789", "+1111111111", 
                "Future message", "PENDING", LocalDateTime.now().plusDays(1), 
                new BigDecimal("0.04"), "nexmo");
            
            smsRepo.insert(futureMessage);
            System.out.println("✅ Successfully inserted message for future date (table auto-created)");
            
            // Graceful shutdown
            System.out.println("\n🔄 Shutting down repository...");
            smsRepo.shutdown();
            System.out.println("✅ Repository shutdown complete");
            
        } catch (SQLException e) {
            System.err.println("❌ Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("❌ Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}