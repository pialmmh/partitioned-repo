package com.telcobright.db.test.entity;

import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.annotation.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * Comprehensive test entity for testing all framework features
 */
@Table(name = "test_data")
public class TestEntity implements ShardingEntity<Long> {
    
    @Id
    @Column(name = "id", insertable = false, updatable = false)
    private Long id;
    
    @Index
    @Column(name = "name", nullable = false)
    private String name;
    
    @Index(name = "idx_email_unique", unique = true)
    @Column(name = "email")
    private String email;
    
    @Index(name = "idx_status_search", comment = "Fast status filtering")
    @Column(name = "status")
    private String status;
    
    @Column(name = "age")
    private Integer age;
    
    @Column(name = "balance")
    private BigDecimal balance;
    
    @Column(name = "active")
    private Boolean active;
    
    @Column(name = "description")
    private String description;
    
    @ShardingKey
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    // Constructors
    public TestEntity() {}
    
    public TestEntity(String name, String email, String status, Integer age, 
                     BigDecimal balance, Boolean active, String description, 
                     LocalDateTime createdAt) {
        this.name = name;
        this.email = email;
        this.status = status;
        this.age = age;
        this.balance = balance;
        this.active = active;
        this.description = description;
        this.createdAt = createdAt;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }
    
    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    
    public Boolean getActive() { return active; }
    public void setActive(Boolean active) { this.active = active; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return String.format("TestEntity{id=%d, name='%s', email='%s', status='%s', " +
                           "age=%d, balance=%s, active=%s, createdAt=%s}",
            id, name, email, status, age, balance, active, createdAt);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        TestEntity that = (TestEntity) o;
        
        if (id != null && that.id != null) {
            return id.equals(that.id);
        }
        
        return name != null ? name.equals(that.name) : that.name == null &&
               email != null ? email.equals(that.email) : that.email == null;
    }
    
    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        return result;
    }
}