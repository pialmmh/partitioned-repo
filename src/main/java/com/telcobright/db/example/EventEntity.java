package com.telcobright.db.example;

import com.telcobright.db.annotation.Column;
import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.annotation.ShardingTable;

import java.time.LocalDateTime;

/**
 * Event Entity for demonstrating PARTITIONED_TABLE mode
 * 
 * Uses single table with date-based partitions:
 * - Logical table: event
 * - Physical partitions: event_20250727, event_20250728, etc.
 */
@ShardingTable(
    value = "event", 
    mode = ShardingMode.PARTITIONED_TABLE,  // Single table with partitions
    shardKey = "timestamp",
    retentionSpanDays = 30,                  // Keep 30 days of data
    autoManagePartition = true,
    partitionAdjustmentTime = "02:00"        // Daily maintenance at 2:00 AM
)
public class EventEntity {
    
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "event_type", indexed = true, nullable = false)
    private String eventType;
    
    @Column(name = "user_id", indexed = true)
    private String userId;
    
    @Column(name = "timestamp", indexed = true)
    private LocalDateTime timestamp;
    
    @Column(name = "data", type = "TEXT")
    private String data;
    
    @Column(name = "severity")
    private String severity;
    
    // Constructors
    public EventEntity() {}
    
    public EventEntity(String eventType, String userId, LocalDateTime timestamp, String data, String severity) {
        this.eventType = eventType;
        this.userId = userId;
        this.timestamp = timestamp;
        this.data = data;
        this.severity = severity;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public String getData() { return data; }
    public void setData(String data) { this.data = data; }
    
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    
    @Override
    public String toString() {
        return "EventEntity{" +
                "id=" + id +
                ", eventType='" + eventType + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", data='" + data + '\'' +
                ", severity='" + severity + '\'' +
                '}';
    }
}