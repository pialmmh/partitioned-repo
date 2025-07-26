package com.telcobright.db.example;

import com.telcobright.db.builder.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.sharding.ShardingMode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BuilderUsageExample {
    
    public static void main(String[] args) {
        DataSource dataSource = createDataSource();
        
        try {
            // Example 1: Partitioned Table Mode
            partitionedTableExample(dataSource);
            
            // Example 2: Multi-Table Mode
            multiTableExample(dataSource);
            
        } finally {
            // Cleanup - this will automatically stop all schedulers
            ShardingRepositoryBuilder.stopAllSchedulers();
            
            if (dataSource instanceof HikariDataSource) {
                ((HikariDataSource) dataSource).close();
            }
        }
    }
    
    private static void partitionedTableExample(DataSource dataSource) {
        System.out.println("=== Partitioned Table Example with Builder ===");
        
        // Single line to create a fully configured repository with auto-initialization
        ShardingRepository<SmsEntity> repository = new ShardingRepositoryBuilder<SmsEntity>()
                .mode(ShardingMode.PARTITIONED_TABLE)
                .entityName("sms")
                .dataSource(dataSource)
                .retentionSpanDays(7)
                .partitionAdjustmentTime(LocalTime.of(4, 0))
                .autoManagePartition(true)
                .withRepositoryFactory(SmsPartitionedRepository::new)
                .build();
        
        // Use the repository - partitions are already initialized, scheduler is running
        LocalDateTime now = LocalDateTime.now();
        SmsEntity sms = new SmsEntity("+1234567890", "Hello from Builder!", "SENT", now, "user1");
        
        repository.insert(sms);
        
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(1), now.plusDays(1));
        System.out.println("Found " + messages.size() + " messages");
    }
    
    private static void multiTableExample(DataSource dataSource) {
        System.out.println("\n=== Multi-Table Example with Builder ===");
        
        // Another single line configuration for multi-table mode
        ShardingRepository<SmsEntity> repository = new ShardingRepositoryBuilder<SmsEntity>()
                .mode(ShardingMode.MULTI_TABLE)
                .entityName("sms_multi")
                .dataSource(dataSource)
                .retentionSpanDays(30)
                .partitionAdjustmentTime(LocalTime.of(2, 0))
                .withRepositoryFactory(config -> new SmsMultiTableRepository(config))
                .build();
        
        // Use the repository
        LocalDateTime now = LocalDateTime.now();
        List<SmsEntity> batch = Arrays.asList(
            new SmsEntity("+1111111111", "Multi Table Test", "DELIVERED", now, "user3"),
            new SmsEntity("+2222222222", "Another Test", "FAILED", now.minusDays(2), "user4")
        );
        
        repository.insertBatch(batch);
        
        List<Map<String, Object>> stats = repository.executeGroupByQuery(
            "user_id, COUNT(*) as message_count",
            null,
            "user_id",
            now.minusDays(3),
            now.plusDays(1)
        );
        
        System.out.println("User stats: " + stats);
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