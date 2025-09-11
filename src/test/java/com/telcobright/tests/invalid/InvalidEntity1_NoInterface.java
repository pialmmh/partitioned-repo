package com.telcobright.tests.invalid;

import java.time.LocalDateTime;

/**
 * Invalid entity that doesn't implement ShardingEntity interface
 * This should cause compilation errors when trying to use with repositories
 */
public class InvalidEntity1_NoInterface {
    
    private Long id;
    private String name;
    private LocalDateTime createdAt;
    
    // Standard getters and setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}