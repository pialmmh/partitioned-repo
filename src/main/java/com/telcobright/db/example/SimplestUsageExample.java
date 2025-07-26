package com.telcobright.db.example;

import com.telcobright.db.builder.ShardingRepositoryFactory;
import com.telcobright.db.repository.ShardingRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.time.LocalDateTime;

public class SimplestUsageExample {
    
    public static void main(String[] args) {
        DataSource dataSource = createDataSource();
        
        // Simplest possible usage - one line to get a fully functional repository
        ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryFactory.create(
            "sms",
            dataSource,
            SmsPartitionedRepository::new
        );
        
        // That's it! Partitions are initialized, scheduler is running
        LocalDateTime now = LocalDateTime.now();
        smsRepo.insert(new SmsEntity("+1234567890", "One line setup!", "SENT", now, "user1"));
        
        System.out.println("Message count: " + smsRepo.count(now.minusDays(1), now.plusDays(1)));
        
        // Example with custom retention
        ShardingRepository<SmsEntity> longTermRepo = ShardingRepositoryFactory.createWithRetention(
            "sms_archive",
            dataSource,
            365, // 1 year retention
            SmsPartitionedRepository::new
        );
        
        // Example with multi-table mode
        ShardingRepository<SmsEntity> multiTableRepo = ShardingRepositoryFactory.createMultiTable(
            "sms_daily",
            dataSource,
            30,
            SmsMultiTableRepository::new
        );
        
        // Cleanup
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }
    
    private static DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=UTC");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTestQuery("SELECT 1");
        
        return new HikariDataSource(config);
    }
}