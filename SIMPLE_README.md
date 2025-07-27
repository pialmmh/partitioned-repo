# 🚀 Generic Sharding-Aware Repository Framework

A **zero-boilerplate**, **framework-independent** library for multi-table sharded data access using **Apache ShardingSphere** with automatic retention management.

## ✨ **Key Features**

- 🧩 **Generic Repository**: `ShardingRepository<Entity>` with auto-generated SQL
- 📊 **Multi-Table Sharding**: Creates separate tables per day (e.g., `sms_20250727`, `sms_20250728`)
- 🕒 **Auto Table Creation**: Tables created automatically on startup for retention window
- ⏰ **Daily Scheduler**: Background cleanup at configurable time (e.g., 04:00)
- 🗑️ **Retention Management**: Automatic deletion of old tables based on retention policy
- 🔍 **Configurable Indexing**: Control which fields get database indexes via annotations
- 🚫 **Framework Independent**: Works with Spring, Quarkus, plain Java

## 🎯 **Simple Usage**

### 1. **Annotate Your Entity**
```java
@ShardingTable(
    value = "sms", 
    mode = ShardingMode.MULTI_TABLE,      // Creates sms_20250727, sms_20250728, etc.
    retentionSpanDays = 7,                // Keep 7 days of data
    autoManagePartition = true,           // Enable automatic management
    partitionAdjustmentTime = "04:00"     // Daily cleanup at 4:00 AM
)
public class SmsEntity {
    
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "phone_number", indexed = true, nullable = false)
    private String phoneNumber;
    
    @Column(name = "created_at", indexed = true)
    private LocalDateTime createdAt;
    
    // ... getters/setters
}
```

### 2. **Super Simple Usage** - Just Repository Type + Entity + MySQL Params

#### **Multi-Table Sharding** (Separate tables per day)
```java
// Create multi-table repository - separate tables: sms_20250727, sms_20250728, etc.
ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
    .multiTable()                    // Repository type: MULTI_TABLE
    .host("localhost")              // MySQL host
    .port(3306)                    // MySQL port  
    .database("mydb")              // Database name
    .username("user")              // Username
    .password("pass")              // Password
    .maxPoolSize(10)               // Optional: connection pool size
    .buildRepository(SmsEntity.class);

// Use it - zero boilerplate!
smsRepo.insert(sms);                              // Auto-routes to correct table
smsRepo.findByDateRange(start, end);             // Queries across all tables
smsRepo.count(start, end);                       // Counts across tables
```

#### **Partitioned Table Sharding** (Single table with partitions)
```java
// Create partitioned repository - single table: event with event_20250727, event_20250728 partitions
ShardingRepository<EventEntity> eventRepo = ShardingRepositoryBuilder
    .partitionedTable()              // Repository type: PARTITIONED_TABLE
    .host("localhost")
    .database("mydb")
    .username("user")
    .password("pass")
    .buildRepository(EventEntity.class);

// Same API - works identically!
eventRepo.insert(event);                         // Auto-routes to correct partition
eventRepo.findByDateRange(start, end);          // Queries across all partitions
eventRepo.count(start, end);                    // Counts across partitions
```

#### **What the Builder Handles Automatically:**
- ✅ **HikariCP DataSource** creation with optimized settings
- ✅ **Entity metadata** extraction from annotations
- ✅ **ShardingSphere configuration** with sharding rules  
- ✅ **Tables/partitions** auto-created for retention window
- ✅ **Daily scheduler** started for maintenance
- ✅ **Repository type** selection (multi-table vs partitioned)

## ⏰ **Automatic Management**

### **On Startup:**
```
✓ Creates tables for ±retentionSpanDays (e.g., ±7 days = 14 tables)
✓ Tables: sms_20250720, sms_20250721, ..., sms_20250727, sms_20250728, etc.
✓ All indexes created automatically based on @Column(indexed=true)
✓ Ready for immediate data insertion
```

### **Daily at 04:00 (configurable):**
```
✓ Creates future tables (e.g., sms_20250804, sms_20250805)
✓ Drops expired tables (e.g., sms_20250713, sms_20250712)
✓ Maintains exactly retentionSpanDays of data
```

## 🔧 **Advanced Usage** (If You Need More Control)

The simplified builder covers most use cases, but you can also use the lower-level APIs:

### **Multiple Entities with Different Repository Types**
```java
// Multi-table repositories (separate tables per day)
ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
    .multiTable()
    .database("mydb").username("user").password("pass")
    .buildRepository(SmsEntity.class);

ShardingRepository<OrderEntity> orderRepo = ShardingRepositoryBuilder
    .multiTable()
    .database("mydb").username("user").password("pass")
    .buildRepository(OrderEntity.class);

// Partitioned table repositories (single table with partitions)
ShardingRepository<EventEntity> eventRepo = ShardingRepositoryBuilder
    .partitionedTable()
    .database("mydb").username("user").password("pass")
    .buildRepository(EventEntity.class);

ShardingRepository<AuditEntity> auditRepo = ShardingRepositoryBuilder
    .partitionedTable()
    .database("mydb").username("user").password("pass")
    .buildRepository(AuditEntity.class);
```

### **Framework Integration**
- **Spring Boot**: Create `@Bean ShardingRepository<T>` using the builder
- **Quarkus**: Use `@ApplicationScoped` producer methods
- **Plain Java**: Use the builder directly in your main() method
- **Any framework**: Builder works everywhere - no dependencies on specific frameworks

## 🚀 **Run Example**

```bash
mvn exec:java -Dexec.mainClass="com.telcobright.db.example.SmsMultiTableExample"
```

---

*Zero-boilerplate multi-table sharding with enterprise-grade retention management.* 🎉