package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Simple test to verify Split-Verse repository is working
 */
public class SimpleSplitVerseTest {
    
    public static void main(String[] args) {
        System.out.println("=== Simple Split-Verse Test ===\n");
        
        try {
            // Configure single shard
            ShardConfig config = ShardConfig.builder()
                .shardId("primary")
                .host("127.0.0.1")
                .port(3306)
                .database("splitverse_test")
                .username("root")
                .password("123456")
                .connectionPoolSize(5)
                .enabled(true)
                .build();
            
            // Create Split-Verse repository with explicit DATE_BASED partitioning
            System.out.println("Creating Split-Verse repository with DATE_BASED partitioning...");
            SplitVerseRepository<SubscriberEntity> repository = 
                SplitVerseRepository.<SubscriberEntity>builder()
                    .withSingleShard(config)
                    .withEntityClass(SubscriberEntity.class)
                    .withPartitionType(PartitionType.DATE_BASED)
                    .withPartitionKeyColumn("created_at")
                    .build();
            
            System.out.println("✓ Repository created successfully\n");
            
            // Create test entity
            String testId = "test_" + System.currentTimeMillis();
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(testId);
            subscriber.setMsisdn("+8801711111111");
            subscriber.setBalance(new BigDecimal("100.00"));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("PREPAID");
            subscriber.setCreatedAt(LocalDateTime.now());
            subscriber.setDataBalanceMb(1000L);
            subscriber.setVoiceBalanceMinutes(100);
            
            // Insert entity
            System.out.println("Inserting test entity with ID: " + testId);
            repository.insert(subscriber);
            System.out.println("✓ Entity inserted successfully\n");
            
            // Find entity
            System.out.println("Finding entity by ID...");
            SubscriberEntity found = repository.findById(testId);
            
            if (found != null) {
                System.out.println("✓ Entity found!");
                System.out.println("  ID: " + found.getId());
                System.out.println("  MSISDN: " + found.getMsisdn());
                System.out.println("  Balance: " + found.getBalance());
                System.out.println("  Status: " + found.getStatus());
            } else {
                System.out.println("✗ Entity not found");
            }
            
            // Cleanup
            System.out.println("\nShutting down repository...");
            repository.shutdown();
            System.out.println("✓ Repository shutdown complete");
            
            System.out.println("\n=== Test Complete ===");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}