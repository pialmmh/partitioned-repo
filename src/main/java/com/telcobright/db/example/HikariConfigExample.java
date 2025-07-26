package com.telcobright.db.example;

import com.telcobright.db.builder.ShardingRepositoryBuilder;
import com.telcobright.db.builder.ShardingRepositoryFactory;
import com.telcobright.db.repository.ShardingRepository;
import com.zaxxer.hikari.HikariConfig;

import java.time.LocalDateTime;

public class HikariConfigExample {
    
    public static void main(String[] args) {
        try {
            // Example 1: Using factory method with HikariConfig
            simplestExample();
            
            // Example 2: MySQL configuration encapsulated
            mysqlExample();
            
        } finally {
            // Cleanup everything - schedulers and data sources
            ShardingRepositoryBuilder.stopAllSchedulers();
        }
    }
    
    private static void simplestExample() {
        System.out.println("=== Simplest Example with HikariConfig ===");
        
        // One-liner with factory method - everything is managed internally
        ShardingRepository<SmsEntity> repository = ShardingRepositoryFactory.createWithHikari(
            "sms",
            createHikariConfig(),
            SmsPartitionedRepository::new
        );
        
        // Use it immediately
        LocalDateTime now = LocalDateTime.now();
        repository.insert(new SmsEntity("+1234567890", "HikariConfig example!", "SENT", now, "user1"));
        
        System.out.println("Message count: " + repository.count(now.minusDays(1), now.plusDays(1)));
    }
    
    private static void mysqlExample() {
        System.out.println("\n=== MySQL Configuration Encapsulated ===");
        
        // Create a pre-configured MySQL HikariConfig
        HikariConfig mysqlConfig = createMySQLConfig("127.0.0.1", 3306, "test", "root", "123456");
        
        // Build repository with all features enabled
        ShardingRepository<SmsEntity> repository = new ShardingRepositoryBuilder<SmsEntity>()
                .entityName("sms_archive")
                .hikariConfig(mysqlConfig)
                .retentionSpanDays(365) // 1 year retention
                .withRepositoryFactory(SmsPartitionedRepository::new)
                .build();
        
        // DataSource is created and managed internally
        LocalDateTime now = LocalDateTime.now();
        repository.insert(new SmsEntity("+9876543210", "MySQL config example", "DELIVERED", now, "user2"));
        
        System.out.println("Archive message count: " + repository.count(now.minusDays(30), now.plusDays(1)));
    }
    
    private static HikariConfig createHikariConfig() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("ShardingPool");
        
        return config;
    }
    
    private static HikariConfig createMySQLConfig(String host, int port, String database, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC", host, port, database));
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("MySQL-" + database);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        return config;
    }
}