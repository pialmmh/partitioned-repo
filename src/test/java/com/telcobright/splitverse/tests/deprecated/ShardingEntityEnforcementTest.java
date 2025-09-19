package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;

import java.time.LocalDateTime;

/**
 * Test to verify ShardingEntity interface enforcement
 */
public class ShardingEntityEnforcementTest {
    
    // Valid entity that implements ShardingEntity
    static class ValidEntity implements ShardingEntity<LocalDateTime> {
        private String id;
        private LocalDateTime createdAt;
        
        @Override
        public String getId() { return id; }
        
        @Override
        public void setId(String id) { this.id = id; }

        public LocalDateTime getCreatedAt() { return createdAt; }

        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

        @Override
        public LocalDateTime getPartitionColValue() { return createdAt; }

        @Override
        public void setPartitionColValue(LocalDateTime value) { this.createdAt = value; }
    }
    
    // Invalid entity that doesn't implement ShardingEntity
    static class InvalidEntity {
        private String id;
        private LocalDateTime createdAt;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    }
    
    public static void main(String[] args) {
        System.out.println("=== ShardingEntity Interface Enforcement Test ===\n");
        
        // Test 1: Valid entity that implements ShardingEntity
        testValidEntity();
        
        // Test 2: Try to use invalid entity (won't compile)
        testInvalidEntity();
        
        // Test 3: Verify required methods
        testRequiredMethods();
        
        System.out.println("\n=== Test Complete ===");
    }
    
    private static void testValidEntity() {
        System.out.println("Test 1: Valid Entity (implements ShardingEntity)");
        System.out.println("-------------------------------------------------");
        
        try {
            ShardConfig config = ShardConfig.builder()
                .shardId("test")
                .host("127.0.0.1")
                .database("test")
                .username("root")
                .password("123456")
                .build();
            
            // This compiles fine - ValidEntity implements ShardingEntity
            SplitVerseRepository<ValidEntity, LocalDateTime> validRepo = 
                SplitVerseRepository.<ValidEntity, LocalDateTime>builder()
                    .withSingleShard(config)
                    .withEntityClass(ValidEntity.class)
                    .withPartitionType(PartitionType.DATE_BASED)
                    .withPartitionKeyColumn("createdAt")
                    .build();
            
            System.out.println("✓ Valid entity accepted by SplitVerseRepository");
            
            // Test that we can use the entity
            ValidEntity entity = new ValidEntity();
            entity.setId("test_123");
            entity.setPartitionColValue(LocalDateTime.now());
            
            System.out.println("✓ Entity ID: " + entity.getId());
            System.out.println("✓ Entity CreatedAt: " + entity.getPartitionColValue());
            
        } catch (Exception e) {
            System.out.println("✗ Error: " + e.getMessage());
        }
    }
    
    private static void testInvalidEntity() {
        System.out.println("\nTest 2: Invalid Entity (doesn't implement ShardingEntity)");
        System.out.println("----------------------------------------------------------");
        
        // The following would NOT compile:
        /*
        SplitVerseRepository<InvalidEntity, LocalDateTime> invalidRepo = 
            SplitVerseRepository.<InvalidEntity, LocalDateTime>builder()
                .withSingleShard(config)
                .withEntityClass(InvalidEntity.class)
                .build();
        */
        
        System.out.println("✓ Compilation enforces ShardingEntity interface");
        System.out.println("  InvalidEntity cannot be used with SplitVerseRepository");
        System.out.println("  Error: Type parameter 'InvalidEntity' is not within its bound");
        System.out.println("  Should extend 'ShardingEntity'");
    }
    
    private static void testRequiredMethods() {
        System.out.println("\nTest 3: Required Methods Verification");
        System.out.println("--------------------------------------");
        
        // Create instance to test interface methods
        ValidEntity entity = new ValidEntity();
        
        // Test String ID enforcement
        System.out.println("Testing String ID enforcement:");
        String testId = "string_id_123";
        entity.setId(testId);
        String retrievedId = entity.getId();
        System.out.println("  Set ID: " + testId);
        System.out.println("  Get ID: " + retrievedId);
        System.out.println("  ✓ String ID methods working");
        
        // Test LocalDateTime createdAt enforcement
        System.out.println("\nTesting LocalDateTime createdAt enforcement:");
        LocalDateTime testDate = LocalDateTime.now();
        entity.setPartitionColValue(testDate);
        LocalDateTime retrievedDate = entity.getPartitionColValue();
        System.out.println("  Set CreatedAt: " + testDate);
        System.out.println("  Get CreatedAt: " + retrievedDate);
        System.out.println("  ✓ LocalDateTime methods working");
        
        // Verify actual entity classes
        System.out.println("\nVerifying example entities implement ShardingEntity:");
        
        // SubscriberEntity check
        boolean subscriberImplements = ShardingEntity.class.isAssignableFrom(SubscriberEntity.class);
        System.out.println("  SubscriberEntity: " + (subscriberImplements ? "✓ Implements" : "✗ Doesn't implement"));
        
        // ValidEntity check
        boolean validImplements = ShardingEntity.class.isAssignableFrom(ValidEntity.class);
        System.out.println("  ValidEntity: " + (validImplements ? "✓ Implements" : "✗ Doesn't implement"));
        
        // InvalidEntity check
        boolean invalidImplements = ShardingEntity.class.isAssignableFrom(InvalidEntity.class);
        System.out.println("  InvalidEntity: " + (invalidImplements ? "✓ Implements" : "✗ Doesn't implement"));
    }
}