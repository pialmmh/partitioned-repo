package com.telcobright.db.entity;

import com.telcobright.db.annotation.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * Order entity for single partitioned table strategy
 * Uses MySQL native partitioning on a single table 'orders'
 * Partitions by date range: p20250803, p20250804, etc.
 */
@Table(name = "orders")
public class OrderEntity {
    
    @Id
    @Column(name = "id", insertable = false, updatable = false)
    private Long id;
    
    @Column(name = "customer_id", nullable = false)
    private String customerId;
    
    @Column(name = "order_number", nullable = false)
    private String orderNumber;
    
    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;
    
    @Column(name = "status", nullable = false)
    private String status; // PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
    
    @Column(name = "payment_method")
    private String paymentMethod;
    
    @Column(name = "shipping_address")
    private String shippingAddress;
    
    @ShardingKey
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "shipped_at")
    private LocalDateTime shippedAt;
    
    @Column(name = "delivered_at")
    private LocalDateTime deliveredAt;
    
    @Column(name = "item_count")
    private Integer itemCount;
    
    // Constructors
    public OrderEntity() {}
    
    public OrderEntity(String customerId, String orderNumber, BigDecimal totalAmount, 
                      String status, String paymentMethod, String shippingAddress,
                      LocalDateTime createdAt, Integer itemCount) {
        this.customerId = customerId;
        this.orderNumber = orderNumber;
        this.totalAmount = totalAmount;
        this.status = status;
        this.paymentMethod = paymentMethod;
        this.shippingAddress = shippingAddress;
        this.createdAt = createdAt;
        this.itemCount = itemCount;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getOrderNumber() { return orderNumber; }
    public void setOrderNumber(String orderNumber) { this.orderNumber = orderNumber; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }
    
    public String getShippingAddress() { return shippingAddress; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getShippedAt() { return shippedAt; }
    public void setShippedAt(LocalDateTime shippedAt) { this.shippedAt = shippedAt; }
    
    public LocalDateTime getDeliveredAt() { return deliveredAt; }
    public void setDeliveredAt(LocalDateTime deliveredAt) { this.deliveredAt = deliveredAt; }
    
    public Integer getItemCount() { return itemCount; }
    public void setItemCount(Integer itemCount) { this.itemCount = itemCount; }
    
    @Override
    public String toString() {
        return String.format("OrderEntity{id=%d, customerId='%s', orderNumber='%s', amount=%s, status='%s', createdAt=%s}", 
            id, customerId, orderNumber, totalAmount, status, createdAt);
    }
}