package com.telcobright.db.example;

import com.telcobright.db.annotation.Column;
import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.annotation.ShardingTable;

import java.time.LocalDateTime;

/**
 * SMS Entity specifically for partitioned table mode testing
 */
@ShardingTable(
    value = "sms", 
    mode = ShardingMode.PARTITIONED_TABLE,   // Single table with date-based partitions
    retentionSpanDays = 7,                   // Keep 7 days of partitions
    partitionAdjustmentTime = "04:00",       // Daily maintenance at 4 AM
    autoManagePartition = true               // Auto create/drop partitions
)
public class SmsPartitionedEntity {
    
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "phone_number", type = "VARCHAR(20)", nullable = false, indexed = true)
    private String phoneNumber;
    
    @Column(type = "TEXT")
    private String message;
    
    @Column(type = "VARCHAR(20)", nullable = false)
    private String status;
    
    @Column(name = "created_at", type = "DATETIME", nullable = false, indexed = true)
    private LocalDateTime createdAt;
    
    @Column(name = "user_id", type = "VARCHAR(50)", indexed = true)
    private String userId;
    
    // Constructors
    public SmsPartitionedEntity() {}
    
    public SmsPartitionedEntity(String phoneNumber, String message, String status, LocalDateTime createdAt, String userId) {
        this.phoneNumber = phoneNumber;
        this.message = message;
        this.status = status;
        this.createdAt = createdAt;
        this.userId = userId;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
}