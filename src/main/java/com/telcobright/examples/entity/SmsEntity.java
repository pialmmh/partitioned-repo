package com.telcobright.examples.entity;

import com.telcobright.core.entity.ShardingEntity;

import com.telcobright.core.annotation.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * SMS entity for multi-table partitioning strategy
 * Each day gets its own table: sms_20250803, sms_20250804, etc.
 */
@Table(name = "sms")
public class SmsEntity implements ShardingEntity<Long> {
    
    @Id
    @Column(name = "id", insertable = false, updatable = false)
    private Long id;
    
    @Column(name = "user_id")
    private String userId;
    
    @Column(name = "phone_number")
    private String phoneNumber;
    
    @Column(name = "message")
    private String message;
    
    @Column(name = "status")
    private String status; // PENDING, SENT, DELIVERED, FAILED
    
    @ShardingKey
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "delivered_at")
    private LocalDateTime deliveredAt;
    
    @Column(name = "cost")
    private BigDecimal cost;
    
    @Column(name = "provider")
    private String provider;
    
    // Constructors
    public SmsEntity() {}
    
    public SmsEntity(String userId, String phoneNumber, String message, String status, 
                    LocalDateTime createdAt, BigDecimal cost, String provider) {
        this.userId = userId;
        this.phoneNumber = phoneNumber;
        this.message = message;
        this.status = status;
        this.createdAt = createdAt;
        this.cost = cost;
        this.provider = provider;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getDeliveredAt() { return deliveredAt; }
    public void setDeliveredAt(LocalDateTime deliveredAt) { this.deliveredAt = deliveredAt; }
    
    public BigDecimal getCost() { return cost; }
    public void setCost(BigDecimal cost) { this.cost = cost; }
    
    public String getProvider() { return provider; }
    public void setProvider(String provider) { this.provider = provider; }
    
    @Override
    public String toString() {
        return String.format("SmsEntity{id=%d, userId='%s', phone='%s', status='%s', createdAt=%s}", 
            id, userId, phoneNumber, status, createdAt);
    }
}