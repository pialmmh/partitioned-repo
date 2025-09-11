package com.telcobright.tests.invalid;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.annotation.*;
import java.time.LocalDateTime;

/**
 * Invalid entity with wrong ID type (String instead of Long)
 * This should cause type mismatch issues
 */
@Table(name = "invalid_wrong_id_type")
public class InvalidEntity4_WrongIdType implements ShardingEntity<String> {
    
    @Id
    @Column(name = "id")
    private String id;  // Using String instead of Long - may cause issues with AUTO_INCREMENT
    
    @Column(name = "name")
    private String name;
    
    @ShardingKey
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}