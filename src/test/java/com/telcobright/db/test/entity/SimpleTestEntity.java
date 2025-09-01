package com.telcobright.db.test.entity;

import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.annotation.*;
import java.time.LocalDateTime;

/**
 * Simple test entity with minimal fields for basic testing
 */
@Table(name = "simple_test")
public class SimpleTestEntity implements ShardingEntity<String> {
    
    @Id
    @Column(name = "test_id", insertable = true, updatable = false)
    private String testId;
    
    @Column(name = "message", nullable = false)
    private String message;
    
    @ShardingKey
    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;
    
    // Constructors
    public SimpleTestEntity() {}
    
    public SimpleTestEntity(String testId, String message, LocalDateTime timestamp) {
        this.testId = testId;
        this.message = message;
        this.timestamp = timestamp;
    }
    
    // Getters and Setters
    public String getTestId() { return testId; }
    public void setTestId(String testId) { this.testId = testId; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    @Override
    public String toString() {
        return String.format("SimpleTestEntity{testId='%s', message='%s', timestamp=%s}",
            testId, message, timestamp);
    }
}