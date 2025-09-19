package com.telcobright.splitverse.examples;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Example demonstrating Split-Verse usage for telecom subscriber management.
 * Shows transition from single shard to multi-shard capability.
 */
public class TelecomSubscriberExample {
    
    public static void main(String[] args) {
        System.out.println("===========================================");
        System.out.println("  Split-Verse Telecom Subscriber Example");
        System.out.println("===========================================\n");
        
        try {
            // Phase 1: Single Shard Demo
            singleShardDemo();
            
            System.out.println("\n===========================================\n");
            
            // Phase 2: Multi-Shard Demo (commented out for now)
            // multiShardDemo();
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void singleShardDemo() throws SQLException {
        System.out.println("PHASE 1: Single Shard Configuration");
        System.out.println("-----------------------------------");
        
        // Configure single shard (wrapping existing partitioned-repo)
        ShardConfig singleShard = ShardConfig.builder()
            .shardId("primary-shard")
            .host("127.0.0.1")
            .port(3306)
            .database("telecom_db")
            .username("root")
            .password("123456")
            .connectionPoolSize(10)
            .build();
        
        // Create Split-Verse repository with single shard
        SplitVerseRepository<SubscriberEntity, LocalDateTime> repository =
            SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                .withSingleShard(singleShard)
                .withEntityClass(SubscriberEntity.class)
                .build();
        
        System.out.println("✓ Repository initialized with single shard\n");
        
        // Generate external IDs (NO AUTO_INCREMENT!)
        String[] subscriberIds = generateSubscriberIds(5);
        
        // 1. Insert subscribers
        System.out.println("1. Inserting subscribers:");
        List<SubscriberEntity> subscribers = new ArrayList<>();
        
        for (int i = 0; i < subscriberIds.length; i++) {
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(subscriberIds[i]);
            subscriber.setMsisdn("+88017" + String.format("%08d", 10000000 + i));
            subscriber.setImsi("47001012345678" + i);
            subscriber.setBalance(new BigDecimal("100.00"));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("PREPAID");
            subscriber.setPartitionColValue(LocalDateTime.now().minusDays(30 - i));
            
            repository.insert(subscriber);
            subscribers.add(subscriber);
            System.out.println("  ✓ Inserted: " + subscriber.getId() + 
                " (MSISDN: " + subscriber.getMsisdn() + ")");
        }
        
        // 2. Find by ID (hash routing in action)
        System.out.println("\n2. Finding subscriber by ID:");
        String searchId = subscriberIds[2];
        SubscriberEntity found = repository.findById(searchId);
        if (found != null) {
            System.out.println("  ✓ Found: " + found.getId() + 
                " - MSISDN: " + found.getMsisdn() + 
                " - Balance: $" + found.getBalance());
        }
        
        // 3. Update balance
        System.out.println("\n3. Updating subscriber balance:");
        found.setBalance(found.getBalance().add(new BigDecimal("50.00")));
        repository.updateById(searchId, found);
        System.out.println("  ✓ Updated balance to: $" + found.getBalance());
        
        // 4. Batch insert
        System.out.println("\n4. Batch inserting CDR-related subscribers:");
        List<SubscriberEntity> batchSubscribers = new ArrayList<>();
        for (int i = 5; i < 8; i++) {
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(generateNanoId());
            subscriber.setMsisdn("+88019" + String.format("%08d", 10000000 + i));
            subscriber.setImsi("47001012345679" + i);
            subscriber.setBalance(new BigDecimal("50.00"));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("POSTPAID");
            subscriber.setPartitionColValue(LocalDateTime.now());
            batchSubscribers.add(subscriber);
        }
        repository.insertMultiple(batchSubscribers);
        System.out.println("  ✓ Batch inserted " + batchSubscribers.size() + " subscribers");
        
        // 5. Date range query (fan-out in multi-shard, direct in single)
        System.out.println("\n5. Finding recent subscribers:");
        List<SubscriberEntity> recent = repository.findAllByDateRange(
            LocalDateTime.now().minusDays(7),
            LocalDateTime.now()
        );
        System.out.println("  ✓ Found " + recent.size() + " subscribers in last 7 days");
        
        // 6. Cursor-based iteration
        System.out.println("\n6. Cursor-based iteration:");
        String cursor = "0";
        SubscriberEntity next = repository.findOneByIdGreaterThan(cursor);
        int count = 0;
        while (next != null && count < 3) {
            System.out.println("  → " + next.getId() + " - " + next.getMsisdn());
            cursor = next.getId();
            next = repository.findOneByIdGreaterThan(cursor);
            count++;
        }
        
        // 7. Demonstrate hash distribution (for future multi-shard)
        System.out.println("\n7. Hash Distribution Analysis:");
        demonstrateHashDistribution(subscriberIds);
        
        // Cleanup
        repository.shutdown();
        System.out.println("\n✓ Repository shutdown complete");
    }
    
    private static void multiShardDemo() throws SQLException {
        System.out.println("PHASE 2: Multi-Shard Configuration (Future)");
        System.out.println("-------------------------------------------");
        
        // Configure multiple shards
        List<ShardConfig> shards = Arrays.asList(
            ShardConfig.builder()
                .shardId("dhaka-01")
                .host("10.0.1.10")
                .port(3306)
                .database("telecom_dhaka")
                .username("root")
                .password("password")
                .build(),
            
            ShardConfig.builder()
                .shardId("chittagong-01")
                .host("10.0.2.10")
                .port(3306)
                .database("telecom_ctg")
                .username("root")
                .password("password")
                .build(),
            
            ShardConfig.builder()
                .shardId("sylhet-01")
                .host("10.0.3.10")
                .port(3306)
                .database("telecom_sylhet")
                .username("root")
                .password("password")
                .enabled(false)  // Can be enabled later
                .build()
        );
        
        // Create Split-Verse repository with multiple shards
        SplitVerseRepository<SubscriberEntity, LocalDateTime> repository =
            SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                .withShardConfigs(shards)
                .withEntityClass(SubscriberEntity.class)
                .build();
        
        System.out.println("✓ Repository initialized with " + shards.size() + " shards");
        
        // The API remains exactly the same!
        // Split-Verse handles routing transparently
    }
    
    /**
     * Generate subscriber IDs using different strategies
     */
    private static String[] generateSubscriberIds(int count) {
        String[] ids = new String[count];
        for (int i = 0; i < count; i++) {
            ids[i] = generateNanoId();
        }
        return ids;
    }
    
    /**
     * Simple NanoID generator (in production, use proper library)
     */
    private static String generateNanoId() {
        String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        StringBuilder id = new StringBuilder("SUB_");
        for (int i = 0; i < 21; i++) {
            id.append(chars.charAt(random.nextInt(chars.length())));
        }
        return id.toString();
    }
    
    /**
     * Demonstrate how IDs would be distributed across shards
     */
    private static void demonstrateHashDistribution(String[] ids) {
        // Simulate distribution across 3 shards
        int[] distribution = new int[3];
        for (String id : ids) {
            int hash = Math.abs(id.hashCode());
            int shardIndex = hash % 3;
            distribution[shardIndex]++;
        }
        
        System.out.println("  If using 3 shards, distribution would be:");
        for (int i = 0; i < distribution.length; i++) {
            System.out.println("    Shard " + i + ": " + distribution[i] + " subscribers");
        }
    }
}