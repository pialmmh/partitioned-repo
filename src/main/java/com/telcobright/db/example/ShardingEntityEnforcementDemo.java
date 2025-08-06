package com.telcobright.db.example;

import com.telcobright.db.annotation.*;
import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.entity.Student;
import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.repository.GenericPartitionedTableRepository;

import java.time.LocalDateTime;

/**
 * Demonstrates the ShardingEntity interface enforcement
 * 
 * This example shows how the interface prevents compilation errors
 * when trying to use entities that don't implement ShardingEntity<K>
 */
public class ShardingEntityEnforcementDemo {
    
    public static void main(String[] args) {
        System.out.println("=== ShardingEntity Interface Enforcement Demo ===");
        
        // ✅ VALID: These entities implement ShardingEntity<Long>
        demonstrateValidEntities();
        
        // ❌ INVALID: Uncomment the following lines to see compilation errors
        // demonstrateInvalidEntity();
        
        System.out.println("✅ All valid entities compiled successfully!");
        System.out.println("Interface enforcement is working correctly.");
    }
    
    /**
     * Demonstrates valid entities that implement ShardingEntity<K>
     */
    private static void demonstrateValidEntities() {
        System.out.println("\n--- Valid Entities (Implement ShardingEntity<Long>) ---");
        
        // ✅ SmsEntity implements ShardingEntity<Long>
        GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
            GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                .database("test")
                .username("root")
                .password("password")
                .build();
        System.out.println("✅ SmsEntity: Repository created successfully");
        smsRepo.shutdown();
        
        // ✅ Student implements ShardingEntity<Long>
        GenericMultiTableRepository<Student, Long> studentRepo = 
            GenericMultiTableRepository.<Student, Long>builder(Student.class, Long.class)
                .database("test")
                .username("root")
                .password("password")
                .build();
        System.out.println("✅ Student: Repository created successfully");
        studentRepo.shutdown();
        
        // ✅ OrderEntity implements ShardingEntity<Long>
        GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = 
            GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                .database("test")
                .username("root")
                .password("password")
                .build();
        System.out.println("✅ OrderEntity: Repository created successfully");
        orderRepo.shutdown();
    }
    
    /**
     * This method demonstrates what happens when you try to use an invalid entity.
     * 
     * UNCOMMENT THE CODE BELOW TO SEE COMPILATION ERRORS:
     * The compiler will prevent you from using entities that don't implement ShardingEntity<K>
     */
    /*
    private static void demonstrateInvalidEntity() {
        System.out.println("\n--- Invalid Entity (Does NOT implement ShardingEntity) ---");
        
        // ❌ This would cause a compilation error because InvalidEntity
        // does not implement ShardingEntity<Long>
        GenericMultiTableRepository<InvalidEntity, Long> invalidRepo = 
            GenericMultiTableRepository.<InvalidEntity, Long>builder(InvalidEntity.class, Long.class)
                .database("test")
                .username("root")  
                .password("password")
                .build();
        
        // Compilation Error:
        // type argument InvalidEntity is not within bounds of type-variable T
        // where T extends ShardingEntity<K>
    }
    */
    
    /**
     * Example of an entity that does NOT implement ShardingEntity<K>
     * This will cause compilation errors if used with ShardingRepository
     */
    public static class InvalidEntity {
        private Long id;
        private String name;
        private LocalDateTime timestamp; // Not named 'created_at'
        
        // Missing required ShardingEntity interface implementation
        // No getId(), setId(), getCreatedAt(), setCreatedAt() methods
        
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public LocalDateTime getTimestamp() { return timestamp; }
        public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    }
    
    /**
     * Example of a properly implemented entity that follows ShardingEntity contract
     */
    @Table(name = "valid_custom")
    public static class ValidCustomEntity implements ShardingEntity<Long> {
        @Id
        @Column(name = "entity_id", insertable = false)
        private Long id;
        
        @Column(name = "custom_field", nullable = false)
        private String customField;
        
        @ShardingKey
        @Column(name = "created_at", nullable = false)
        private LocalDateTime createdAt;
        
        public ValidCustomEntity() {}
        
        public ValidCustomEntity(String customField, LocalDateTime createdAt) {
            this.customField = customField;
            this.createdAt = createdAt;
        }
        
        // Standard getters and setters (no interface methods required with marker interface)
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        
        // Custom fields
        public String getCustomField() { return customField; }
        public void setCustomField(String customField) { this.customField = customField; }
        
        @Override
        public String toString() {
            return String.format("ValidCustomEntity{id=%d, customField='%s', createdAt=%s}", 
                               id, customField, createdAt);
        }
    }
}