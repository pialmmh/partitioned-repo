package com.telcobright.splitverse.entity.jpa;

import javax.persistence.*;
import com.telcobright.core.annotation.ShardingKey;
import java.time.LocalDateTime;

/**
 * JPA-enabled entity with Split-Verse sharding annotations.
 * Demonstrates dual annotation approach for db-util integration.
 */
@Entity  // JPA annotation
@Table(name = "test_users")  // JPA annotation
public class TestUser {

    @Id  // JPA annotation
    @Column(name = "user_id", nullable = false, length = 100)
    @ShardingKey  // Split-Verse custom annotation for routing
    private String userId;

    @Column(name = "username", nullable = false, unique = true)
    private String username;

    @Column(name = "email", nullable = false)
    private String email;

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    @Column(name = "active")
    private Boolean active = true;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Constructors
    public TestUser() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    public TestUser(String userId, String username, String email) {
        this.userId = userId;
        this.username = username;
        this.email = email;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    // Getters and setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
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
        return "TestUser{" +
            "userId='" + userId + '\'' +
            ", username='" + username + '\'' +
            ", email='" + email + '\'' +
            ", active=" + active +
            ", createdAt=" + createdAt +
            '}';
    }
}