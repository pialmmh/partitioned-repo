package com.telcobright.tests.invalid;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.annotation.*;
import java.time.LocalDateTime;

/**
 * Invalid entity with ShardingEntity interface but missing @Id field
 * This should cause runtime errors during metadata extraction
 */
@Table(name = "invalid_no_id")
public class InvalidEntity2_NoId implements ShardingEntity<Long> {
    
    // Missing @Id annotation - this is required!
    private Long someField;
    
    @Column(name = "name")
    private String name;
    
    @ShardingKey
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    // This will return null since we don't have a proper ID field
    public Long getId() {
        return null;
    }
    
    public void setId(Long id) {
        // Can't set ID - no field annotated with @Id
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