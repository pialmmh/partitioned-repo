package com.telcobright.db.example;

import com.telcobright.db.annotation.Column;
import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.annotation.ShardingTable;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Order Entity with partitioned table sharding for high-volume order data
 */
@ShardingTable(
    value = "orders", 
    mode = ShardingMode.PARTITIONED_TABLE,   // Single table with MySQL native partitions
    retentionSpanDays = 7,                   // Keep 7 days of data
    autoManagePartition = true,              // Enable automatic partition management
    partitionAdjustmentTime = "04:00"        // Daily maintenance at 4:00 AM
)
public class OrderEntity {
    
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "order_number", type = "VARCHAR(50)", nullable = false, indexed = true)
    private String orderNumber;
    
    @Column(name = "customer_id", type = "VARCHAR(50)", nullable = false, indexed = true)
    private String customerId;
    
    @Column(name = "total_amount", type = "DECIMAL(10,2)", nullable = false)
    private BigDecimal totalAmount;
    
    @Column(type = "VARCHAR(20)", nullable = false)
    private String status;
    
    @Column(name = "created_at", type = "DATETIME", nullable = false, indexed = true)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at", type = "DATETIME", indexed = true)
    private LocalDateTime updatedAt;

    public OrderEntity() {}

    public OrderEntity(String orderNumber, String customerId, BigDecimal totalAmount, String status, LocalDateTime createdAt) {
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "OrderEntity{" +
                "id=" + id +
                ", orderNumber='" + orderNumber + '\'' +
                ", customerId='" + customerId + '\'' +
                ", totalAmount=" + totalAmount +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}