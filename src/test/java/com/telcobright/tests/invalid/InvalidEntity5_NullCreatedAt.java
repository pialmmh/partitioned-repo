package com.telcobright.tests.invalid;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.annotation.*;
import java.time.LocalDateTime;

/**
 * Entity that returns null for created_at
 * This should cause NullPointerException when determining partition/table
 */
@Table(name = "invalid_null_created_at")
public class InvalidEntity5_NullCreatedAt implements ShardingEntity<Long> {
    
    @Id
    @Column(name = "id")
    private Long id;
    
    @Column(name = "name")
    private String name;
    
    @ShardingKey
    @Column(name = "created_at")
    private LocalDateTime createdAt;  // Will be null if not set
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public LocalDateTime getCreatedAt() {
        // Always returns null - will cause issues!
        return null;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        // Intentionally don't set it
        // this.createdAt = createdAt;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}