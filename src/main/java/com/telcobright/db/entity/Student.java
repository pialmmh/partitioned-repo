package com.telcobright.db.entity;

import com.telcobright.db.annotation.Column;
import com.telcobright.db.annotation.Id;
import com.telcobright.db.annotation.ShardingKey;
import com.telcobright.db.annotation.Table;

import java.time.LocalDateTime;


@Table(name = "student")
public class Student implements ShardingEntity<Long> {
    @Id
    @Column(name = "id", updatable = false)
    private Long id;

    @Column(name = "first_name", nullable = false)
    private String firstName;

    @Column(name = "age")
    private Integer age;

    @Column(name = "email", nullable = false)
    private String email;

    @Column(name = "phone")
    private String phone;

    @ShardingKey
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    public Student(String firstName, Integer age, String email, String phone, LocalDateTime createdAt) {
        this.firstName = firstName;
        this.age = age;
        this.email = email;
        this.phone = phone;
        this.createdAt = createdAt;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setName(String firstName) {
        this.firstName = firstName;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return firstName;
    }

    public Integer getAge() {
        return age;
    }

    public String getEmail() {
        return email;
    }

    public String getPhone() {
        return phone;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
