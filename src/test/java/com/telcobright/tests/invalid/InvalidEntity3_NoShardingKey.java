package com.telcobright.tests.invalid;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.annotation.*;
import java.time.LocalDateTime;

/**
 * Invalid entity missing @ShardingKey annotation on created_at field
 * This should cause runtime errors when trying to determine partitioning
 */
@Table(name = "invalid_no_sharding_key")
public class InvalidEntity3_NoShardingKey implements ShardingEntity<Long> {
    
    @Id
    @Column(name = "id")
    private Long id;
    
    @Column(name = "name")
    private String name;
    
    // Missing @ShardingKey annotation - this is required for partitioning!
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
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