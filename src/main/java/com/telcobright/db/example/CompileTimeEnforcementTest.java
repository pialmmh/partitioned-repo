package com.telcobright.db.example;

import com.telcobright.db.annotation.*;
import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.entity.Student;
import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.metadata.EntityMetadata;

import java.time.LocalDateTime;

/**
 * Compile-time test to verify ShardingEntity interface enforcement
 * 
 * This class demonstrates that:
 * 1. Valid entities (that implement ShardingEntity<K>) can be used as type parameters
 * 2. Invalid entities (that don't implement ShardingEntity<K>) cause compilation errors
 */
public class CompileTimeEnforcementTest {
    
    /**
     * Test method that verifies interface constraints at compile time
     */
    public static void testShardingEntityEnforcement() {
        System.out.println("=== Compile-Time ShardingEntity Enforcement Test ===");
        
        // ✅ VALID: These work because entities implement ShardingEntity<Long>  
        SmsEntity sms = new SmsEntity();
        sms.setCreatedAt(LocalDateTime.now());
        testValidEntity(sms);
        
        Student student = new Student("John", 25, "john@example.com", "+1234567890", LocalDateTime.now());
        testValidEntity(student);
        
        OrderEntity order = new OrderEntity();
        order.setCreatedAt(LocalDateTime.now());
        testValidEntity(order);
        
        CustomValidEntity customEntity = new CustomValidEntity("test", LocalDateTime.now());
        testValidEntity(customEntity);
        
        // ❌ INVALID: Uncomment the line below to see a compilation error
        // testValidEntity(new InvalidEntity()); // This won't compile!
        
        System.out.println("✅ All valid entities passed compile-time checks!");
    }
    
    /**
     * Generic method that accepts only entities implementing ShardingEntity<Long>
     * This method signature enforces the constraint at compile time.
     * Uses EntityMetadata to access ID and sharding key values via annotations.
     */
    public static <T extends ShardingEntity<Long>> void testValidEntity(T entity) {
        try {
            // Use EntityMetadata to access fields via annotations
            @SuppressWarnings("unchecked")
            EntityMetadata<T, Long> metadata = (EntityMetadata<T, Long>) 
                new EntityMetadata<>(entity.getClass(), Long.class);
                
            Long id = metadata.getId(entity);
            LocalDateTime shardingKeyValue = metadata.getShardingKeyValue(entity);
            
            System.out.printf("✅ Valid entity: %s (ID: %s, ShardingKey: %s)%n", 
                             entity.getClass().getSimpleName(), id, shardingKeyValue);
        } catch (Exception e) {
            System.out.printf("❌ Error accessing entity fields: %s%n", e.getMessage());
        }
    }
    
    /**
     * Example of a properly implemented custom entity using annotations
     */
    @Table(name = "custom_test")
    public static class CustomValidEntity implements ShardingEntity<Long> {
        @Id
        @Column(name = "custom_id", insertable = false)
        private Long customId;
        
        @Column(name = "entity_name", nullable = false)
        private String name;
        
        @ShardingKey
        @Column(name = "creation_timestamp", nullable = false)
        private LocalDateTime creationTimestamp;
        
        public CustomValidEntity() {}
        
        public CustomValidEntity(String name, LocalDateTime creationTimestamp) {
            this.name = name;
            this.creationTimestamp = creationTimestamp;
        }
        
        // Standard getters and setters (no interface methods required)
        public Long getCustomId() { return customId; }
        public void setCustomId(Long customId) { this.customId = customId; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public LocalDateTime getCreationTimestamp() { return creationTimestamp; }
        public void setCreationTimestamp(LocalDateTime creationTimestamp) { this.creationTimestamp = creationTimestamp; }
    }
    
    /**
     * Example of an invalid entity that does NOT implement ShardingEntity
     * If you try to use this with testValidEntity(), it will cause a compilation error
     */
    public static class InvalidEntity {
        private Long id;
        private String name;
        private LocalDateTime timestamp; // Wrong field name - should be 'createdAt'
        
        // Missing ShardingEntity interface implementation!
        
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public LocalDateTime getTimestamp() { return timestamp; }
        public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    }
    
    public static void main(String[] args) {
        testShardingEntityEnforcement();
    }
}