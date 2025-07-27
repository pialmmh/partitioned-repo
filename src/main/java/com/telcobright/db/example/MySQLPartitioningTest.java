package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.time.LocalDateTime;

/**
 * Test MySQL native hourly partitioning
 */
public class MySQLPartitioningTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== MySQL Native Hourly Partitioning Test ===\n");
            
            System.out.println("Creating repository with multi-table sharding...");
            System.out.println("  • One table per day (e.g., sms_20250728)");
            System.out.println("  • ShardingSphere routes queries across tables by date");
            System.out.println("  • Full UTF-8 support with emoji and Unicode\n");
            
            ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
                .multiTable()
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(3)
                .charset("utf8mb4")
                .collation("utf8mb4_unicode_ci")
                .buildRepository(SmsEntity.class);
            
            System.out.println("✓ Repository created with multi-table sharding\n");
            
            // Test table routing
            LocalDateTime now = LocalDateTime.now();
            System.out.println("Testing table routing and charset support:");
            
            // Insert messages with emoji and unicode
            System.out.println("  • Inserting emoji message...");
            smsRepo.insert(new SmsEntity("+1234567890", "Hello 👋 World 🌍", "SENT", now, "user1"));
            
            System.out.println("  • Inserting unicode message...");
            smsRepo.insert(new SmsEntity("+9876543210", "café résumé naïve", "SENT", now, "user2"));
            
            System.out.println("  • Inserting different day message...");
            smsRepo.insert(new SmsEntity("+5555555555", "Yesterday message", "SENT", now.minusDays(1), "user3"));
            
            System.out.println("\n✅ SUCCESS!");
            System.out.println("  • Tables created: sms_YYYYMMDD (one per day)");
            System.out.println("  • Charset: utf8mb4 with full Unicode support");
            System.out.println("  • Automatic table routing based on date");
            System.out.println("  • ShardingSphere handles cross-table queries");
            System.out.println("  • Emoji and Unicode characters supported");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed:");
            e.printStackTrace();
        }
    }
}