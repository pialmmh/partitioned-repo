package com.telcobright.db.entity;

import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * SMS entity for multi-table partitioning strategy
 * Each day gets its own table: sms_20250803, sms_20250804, etc.
 */
public class SmsEntity {
    
    private Long id;
    private String userId;
    private String phoneNumber;
    private String message;
    private String status; // PENDING, SENT, DELIVERED, FAILED
    private LocalDateTime createdAt;
    private LocalDateTime deliveredAt;
    private BigDecimal cost;
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