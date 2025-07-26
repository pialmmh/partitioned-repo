package com.telcobright.db.example;

import com.telcobright.db.builder.ShardingRepositoryBuilder;
import com.telcobright.db.builder.ShardingRepositoryFactory;
import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.sharding.ShardingMode;
import com.zaxxer.hikari.HikariConfig;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UsageExample {
    
    public static void main(String[] args) {
        try {
            // Example 1: Simplest usage with factory
            simplestExample();
            
            // Example 2: Custom configuration with builder
            customConfigExample();
            
            // Example 3: Multi-table with specific retention
            multiTableExample();
            
        } finally {
            // Cleanup all schedulers and managed data sources
            ShardingRepositoryBuilder.stopAllSchedulers();
        }
    }
    
    private static void simplestExample() {
        System.out.println("=== Simplest Example - Factory Method ===");
        
        // One line setup with factory - everything managed internally
        ShardingRepository<SmsEntity> repository = ShardingRepositoryFactory.createWithHikari(
                "sms",
                createHikariConfig(),
                SmsPartitionedRepository::new
        );
        
        // Use immediately - no setup required
        LocalDateTime now = LocalDateTime.now();
        repository.insert(new SmsEntity("+1234567890", "Factory method example", "SENT", now, "user1"));
        
        long count = repository.count(now.minusDays(1), now.plusDays(1));
        System.out.println("Message count: " + count);
    }
    
    private static void customConfigExample() {
        System.out.println("\n=== Custom Configuration Example ===");
        
        // Using builder for more control
        ShardingRepository<SmsEntity> repository = new ShardingRepositoryBuilder<SmsEntity>()
                .entityName("sms_custom")
                .shardKey("created_at")
                .retentionSpanDays(14) // 2 weeks retention
                .partitionAdjustmentTime(LocalTime.of(3, 30)) // 3:30 AM
                .hikariConfig(createHikariConfig())
                .withRepositoryFactory(SmsPartitionedRepository::new)
                .build();
        
        LocalDateTime now = LocalDateTime.now();
        List<SmsEntity> batch = Arrays.asList(
            new SmsEntity("+1111111111", "Custom config test 1", "SENT", now, "user1"),
            new SmsEntity("+2222222222", "Custom config test 2", "DELIVERED", now.minusDays(1), "user2")
        );
        
        repository.insertBatch(batch);
        
        // Group by query example
        List<Map<String, Object>> stats = repository.executeGroupByQuery(
                "user_id, COUNT(*) as message_count",
                "status IN ('SENT', 'DELIVERED')",
                "user_id",
                now.minusDays(7),
                now.plusDays(1)
        );
        
        System.out.println("User stats: " + stats);
    }
    
    private static void multiTableExample() {
        System.out.println("\n=== Multi-Table Example ===");
        
        // Multi-table mode with long retention
        ShardingRepository<SmsEntity> repository = new ShardingRepositoryBuilder<SmsEntity>()
                .mode(ShardingMode.MULTI_TABLE)
                .entityName("sms_daily")
                .retentionSpanDays(90) // 3 months retention
                .hikariConfig(createHikariConfig())
                .withRepositoryFactory(SmsMultiTableRepository::new)
                .build();
        
        LocalDateTime now = LocalDateTime.now();
        
        // Insert messages across different days
        repository.insert(new SmsEntity("+3333333333", "Today's message", "SENT", now, "user3"));
        repository.insert(new SmsEntity("+4444444444", "Yesterday's message", "DELIVERED", now.minusDays(1), "user4"));
        repository.insert(new SmsEntity("+5555555555", "Last week's message", "FAILED", now.minusDays(7), "user3"));
        
        // Query across multiple tables
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(10), now.plusDays(1));
        System.out.println("Found " + messages.size() + " messages across " + 11 + " daily tables");
        
        // Daily statistics
        List<Map<String, Object>> dailyStats = repository.executeGroupByQuery(
                "DATE(created_at) as date, COUNT(*) as total, " +
                "SUM(CASE WHEN status = 'SENT' THEN 1 ELSE 0 END) as sent, " +
                "SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) as delivered, " +
                "SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed",
                null,
                "DATE(created_at)",
                now.minusDays(10),
                now.plusDays(1)
        );
        
        System.out.println("Daily statistics:");
        dailyStats.forEach(stat -> System.out.println("  " + stat));
    }
    
    private static HikariConfig createHikariConfig() {
        HikariConfig config = new HikariConfig();
        
        // MySQL configuration
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        
        // Connection pool settings
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000); // 30 seconds
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("ShardingPool");
        
        return config;
    }
}