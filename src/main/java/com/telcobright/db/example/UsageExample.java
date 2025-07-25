package com.telcobright.db.example;

import com.telcobright.db.repository.ShardingRepository;
import com.telcobright.db.service.PartitionSchedulerService;
import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.sharding.ShardingMode;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UsageExample {
    
    public static void main(String[] args) {
        DataSource dataSource = createDataSource();
        
        partitionedTableExample(dataSource);
        
        multiTableExample(dataSource);
    }
    
    private static void partitionedTableExample(DataSource dataSource) {
        System.out.println("=== Partitioned Table Example ===");
        
        ShardingConfig config = ShardingConfig.builder()
                .mode(ShardingMode.PARTITIONED_TABLE)
                .entityName("sms")
                .shardKey("created_at")
                .retentionSpanDays(7)
                .partitionAdjustmentTime(LocalTime.of(4, 0))
                .dataSource(dataSource)
                .autoManagePartition(true)
                .build();
        
        ShardingRepository<SmsEntity> repository = new SmsPartitionedRepository(config);
        
        repository.initializePartitions();
        
        PartitionSchedulerService scheduler = new PartitionSchedulerService(config);
        scheduler.start();
        
        LocalDateTime now = LocalDateTime.now();
        SmsEntity sms1 = new SmsEntity("+1234567890", "Hello World", "SENT", now, "user1");
        SmsEntity sms2 = new SmsEntity("+0987654321", "Test Message", "PENDING", now.minusHours(1), "user2");
        
        repository.insert(sms1);
        repository.insertBatch(Arrays.asList(sms1, sms2));
        
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(1), now.plusDays(1));
        System.out.println("Found " + messages.size() + " messages");
        
        List<Map<String, Object>> stats = repository.executeGroupByQuery(
                "user_id, COUNT(*) as message_count",
                "status = 'SENT'",
                "user_id",
                now.minusDays(1),
                now.plusDays(1)
        );
        
        System.out.println("User message stats: " + stats);
        
        long count = repository.count(now.minusDays(1), now.plusDays(1));
        System.out.println("Total messages: " + count);
        
        scheduler.stop();
    }
    
    private static void multiTableExample(DataSource dataSource) {
        System.out.println("\n=== Multi-Table Example ===");
        
        ShardingConfig config = ShardingConfig.builder()
                .mode(ShardingMode.MULTI_TABLE)
                .entityName("sms")
                .shardKey("created_at")
                .retentionSpanDays(30)
                .partitionAdjustmentTime(LocalTime.of(2, 0))
                .dataSource(dataSource)
                .autoManagePartition(true)
                .build();
        
        ShardingRepository<SmsEntity> repository = new SmsMultiTableRepository(config);
        
        repository.initializePartitions();
        
        PartitionSchedulerService scheduler = new PartitionSchedulerService(config);
        scheduler.start();
        
        LocalDateTime now = LocalDateTime.now();
        SmsEntity sms1 = new SmsEntity("+1111111111", "Multi Table Test", "DELIVERED", now, "user3");
        SmsEntity sms2 = new SmsEntity("+2222222222", "Another Test", "FAILED", now.minusDays(2), "user4");
        
        repository.insert(sms1);
        repository.insertBatch(Arrays.asList(sms1, sms2));
        
        List<SmsEntity> messages = repository.findByDateRange(now.minusDays(3), now.plusDays(1));
        System.out.println("Found " + messages.size() + " messages across multiple tables");
        
        List<Map<String, Object>> dailyStats = repository.executeGroupByQuery(
                "DATE(created_at) as message_date, status, COUNT(*) as count",
                null,
                "DATE(created_at), status",
                now.minusDays(7),
                now.plusDays(1)
        );
        
        System.out.println("Daily message stats: " + dailyStats);
        
        scheduler.stop();
    }
    
    private static DataSource createDataSource() {
        return null;
    }
}