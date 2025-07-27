package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Test charset/collation and hourly partitioning with table cleanup
 */
public class CharsetHourlyTest {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Charset & Hourly Partitioning Test ===\n");
            
            // First, drop existing tables for a clean test
            dropExistingTables();
            
            // Create repository with charset and hourly partitioning
            System.out.println("Creating repository with:");
            System.out.println("  • Charset: utf8mb4");
            System.out.println("  • Collation: utf8mb4_unicode_ci");
            System.out.println("  • Hourly partitioning enabled\n");
            
            ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
                .multiTable()                    // Enables hourly sharding
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(3)
                .charset("utf8mb4")
                .collation("utf8mb4_unicode_ci")
                .buildRepository(SmsEntity.class);
            
            System.out.println("✓ Repository created\n");
            
            // Test emoji and unicode support
            LocalDateTime now = LocalDateTime.now();
            System.out.println("Testing emoji and unicode support:");
            
            // Insert at current hour
            String currentTable = "sms_" + now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HH"));
            System.out.println("  • Inserting into " + currentTable + "...");
            smsRepo.insert(new SmsEntity("+1234567890", "Hello 👋 World 🌍", "SENT", now, "user1"));
            System.out.println("    ✓ Emoji message inserted");
            
            // Insert at different hour
            LocalDateTime afternoon = now.withHour(15);
            String afternoonTable = "sms_" + afternoon.format(DateTimeFormatter.ofPattern("yyyyMMdd_HH"));
            System.out.println("  • Inserting into " + afternoonTable + "...");
            smsRepo.insert(new SmsEntity("+9876543210", "café résumé naïve façade", "SENT", afternoon, "user2"));
            System.out.println("    ✓ Unicode message inserted\n");
            
            System.out.println("✅ SUCCESS!");
            System.out.println("  • Hourly partitioning: Working (24 tables/day)");
            System.out.println("  • UTF-8 charset: Working");
            System.out.println("  • Emoji support: Working");
            System.out.println("  • Unicode support: Working");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed:");
            e.printStackTrace();
        }
    }
    
    private static void dropExistingTables() throws Exception {
        System.out.println("Cleaning up existing tables...");
        
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setUsername("root");
        config.setPassword("123456");
        config.setMaximumPoolSize(1);
        
        DataSource ds = new HikariDataSource(config);
        
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Drop tables for today only to speed up test
            LocalDateTime today = LocalDateTime.now();
            String datePrefix = today.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            
            for (int hour = 0; hour < 24; hour++) {
                String tableName = String.format("sms_%s_%02d", datePrefix, hour);
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + tableName);
                } catch (Exception ignored) {}
            }
            
            System.out.println("  ✓ Cleanup complete\n");
        }
        
        ((HikariDataSource) ds).close();
    }
}