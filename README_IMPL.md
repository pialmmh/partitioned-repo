# 🚀 Generic Sharding-Aware Repository Framework

A powerful, framework-independent library for managing sharded and partitioned data using **Apache ShardingSphere**. Supports automatic time-based partitioning, table management, and transparent query routing across shards.

## 📋 Table of Contents
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Core Concepts](#-core-concepts)
- [Usage Examples](#-usage-examples)
- [Advanced Features](#-advanced-features)
- [Configuration Options](#-configuration-options)
- [Best Practices](#-best-practices)

## ✨ Features

- **🔄 Dual Storage Modes**: Multi-table sharding and MySQL native partitioning
- **🤖 Automatic Management**: Creates/drops tables and partitions based on retention policy
- **🔍 Transparent Routing**: ShardingSphere handles all query routing automatically
- **📊 Complex Query Support**: GROUP BY, aggregations, and UNION ALL handled transparently
- **🛡️ SQL Injection Protection**: Safe raw SQL API with parameterized queries
- **⏰ Scheduled Maintenance**: Configurable daily cleanup at specified times
- **🔧 Framework Agnostic**: Works with plain Java, Spring Boot, Quarkus, etc.
- **💾 Connection Pooling**: Built-in HikariCP with configurable timeouts
- **🌍 Full Unicode Support**: UTF-8mb4 charset with proper collation

## 🚀 Quick Start

### Multi-Table Mode (Separate tables per day)
```java
// Define entity with multi-table sharding
@ShardingTable(
    value = "sms",
    mode = ShardingMode.MULTI_TABLE,
    retentionSpanDays = 7,
    autoManagePartition = true,
    partitionAdjustmentTime = "04:00"
)
public class SmsEntity {
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "created_at", type = "DATETIME", nullable = false, indexed = true)
    private LocalDateTime createdAt;
    
    // Other fields...
}

// Create repository
ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
    .multiTable()
    .host("localhost")
    .port(3306)
    .database("mydb")
    .username("user")
    .password("pass")
    .buildRepository(SmsEntity.class);

// Use it - ShardingSphere handles routing!
smsRepo.insert(smsEntity);
List<SmsEntity> results = smsRepo.findByDateRange(startDate, endDate);
```

### Partitioned Table Mode (Single table with MySQL partitions)
```java
// Define entity with partitioned table
@ShardingTable(
    value = "orders",
    mode = ShardingMode.PARTITIONED_TABLE,
    retentionSpanDays = 7
)
public class OrderEntity {
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(name = "created_at", type = "DATETIME", nullable = false, indexed = true)
    private LocalDateTime createdAt;
    
    @Column(name = "total_amount", type = "DECIMAL(10,2)")
    private BigDecimal totalAmount;
}

// Create repository
ShardingRepository<OrderEntity> orderRepo = ShardingRepositoryBuilder
    .partitionedTable()
    .host("localhost")
    .database("mydb")
    .username("user")
    .password("pass")
    .buildRepository(OrderEntity.class);
```

## 📦 Installation

### Maven
```xml
<repositories>
    <repository>
        <id>pialmmh-github-repo</id>
        <url>https://pialmmh.github.io/maven-repo</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.telcobright</groupId>
        <artifactId>sharding-repository</artifactId>
        <version>1.0.0</version>
    </dependency>
    
    <!-- ShardingSphere (from custom repo) -->
    <dependency>
        <groupId>org.apache.shardingsphere</groupId>
        <artifactId>shardingsphere-all-in-one</artifactId>
        <version>5.5.3</version>
    </dependency>
</dependencies>
```

## 🔑 Core Concepts

### Storage Modes

#### 1. Multi-Table Mode
- Creates separate physical tables: `sms_20250726`, `sms_20250727`, etc.
- Best for: High-volume inserts, independent table management
- ShardingSphere automatically generates UNION ALL queries

#### 2. Partitioned Table Mode  
- Single logical table with MySQL native partitions
- Best for: Complex queries, foreign key relationships
- Requires composite primary key (id, created_at)

### Automatic Table Management

The framework automatically:
- Creates tables/partitions for the retention window on startup
- Runs daily maintenance at configured time
- Drops expired tables/partitions
- Creates future tables/partitions

## 📚 Usage Examples

### Basic CRUD Operations

```java
// Insert
SmsEntity sms = new SmsEntity();
sms.setPhoneNumber("+1234567890");
sms.setMessage("Hello World");
sms.setCreatedAt(LocalDateTime.now());
smsRepo.insert(sms);

// Find by date range - ShardingSphere routes automatically!
List<SmsEntity> messages = smsRepo.findByDateRange(
    LocalDateTime.now().minusDays(7),
    LocalDateTime.now()
);

// Count records
long count = smsRepo.count(startDate, endDate);

// Find by ID (scans all tables in retention window)
SmsEntity found = smsRepo.findById(123L);

// Custom queries
List<SmsEntity> results = smsRepo.query(
    "status = ? AND user_id = ?", 
    "sent", "user123"
);
```

### Raw SQL API (Complex Queries)

```java
// Complex aggregation - ShardingSphere handles the routing!
String sql = """
    SELECT user_id, COUNT(*) as sms_count
    FROM sms
    WHERE created_at BETWEEN ? AND ?
    GROUP BY user_id
    ORDER BY sms_count DESC
    """;

List<UserStats> stats = smsRepo.executeRawQuery(
    sql,
    new Object[]{startDate, endDate},
    rs -> new UserStats(
        rs.getString("user_id"),
        rs.getLong("sms_count")
    )
);

// Multiple date ranges with OR
String multiRangeQuery = """
    SELECT user_id, COUNT(*) as count
    FROM sms
    WHERE (created_at BETWEEN ? AND ?)
       OR (created_at BETWEEN ? AND ?)
    GROUP BY user_id
    """;

// Update operations
String updateSql = """
    UPDATE orders
    SET status = ?, updated_at = ?
    WHERE status = ? AND created_at < ?
    """;

int updated = orderRepo.executeRawUpdate(
    updateSql,
    new Object[]{"expired", LocalDateTime.now(), "pending", weekAgo}
);
```

### Control Table Creation

```java
// Disable automatic table creation on startup
ShardingRepository<SmsEntity> repo = ShardingRepositoryBuilder
    .multiTable()
    .host("localhost")
    .database("mydb")
    .username("user")
    .password("pass")
    .createTablesOnStartup(false)  // Disable startup creation
    .buildRepository(SmsEntity.class);

// Entity with manual management
@ShardingTable(
    value = "manual_table",
    mode = ShardingMode.MULTI_TABLE,
    autoManagePartition = false  // No auto creation or cleanup
)
public class ManualEntity {
    // Fields...
}
```

### Connection Configuration

```java
ShardingRepository<SmsEntity> repo = ShardingRepositoryBuilder
    .multiTable()
    .host("localhost")
    .port(3306)
    .database("mydb")
    .username("user")
    .password("pass")
    .maxPoolSize(20)              // Connection pool size
    .connectionTimeout(120000)     // 2 minutes for DDL operations
    .charset("utf8mb4")           // Full Unicode support
    .collation("utf8mb4_unicode_ci")
    .buildRepository(SmsEntity.class);
```

## 🔧 Advanced Features

### 1. Automatic Partition Management

```java
@ShardingTable(
    value = "events",
    retentionSpanDays = 30,           // Keep 30 days of data
    autoManagePartition = true,       // Enable auto management
    partitionAdjustmentTime = "02:00" // Run maintenance at 2 AM
)
```

**What happens automatically:**
- On startup: Creates tables/partitions for -30 to +30 days
- Daily at 2 AM: 
  - Creates tomorrow's table/partition
  - Drops tables/partitions older than 30 days

### 2. Manual Maintenance Trigger

```java
// Get the factory
ShardingRepositoryFactory factory = ShardingRepositoryBuilder
    .multiTable()
    .database("mydb")
    .username("user")
    .password("pass")
    .buildFactory();

// Trigger maintenance manually
factory.getPartitionService().triggerMaintenance(SmsEntity.class);
```

### 3. Entity Field Types

```java
@ShardingTable(value = "complex_entity")
public class ComplexEntity {
    @Column(primaryKey = true, type = "BIGINT AUTO_INCREMENT")
    private Long id;
    
    @Column(type = "VARCHAR(100)", nullable = false)
    private String name;
    
    @Column(type = "DECIMAL(10,2)")
    private BigDecimal amount;
    
    @Column(type = "TEXT")
    private String description;
    
    @Column(type = "DATETIME", indexed = true)
    private LocalDateTime createdAt;
    
    @Column(type = "BOOLEAN", defaultValue = "false")
    private Boolean active;
}
```

### 4. ShardingSphere Query Transformation

When you write:
```sql
SELECT * FROM sms WHERE created_at BETWEEN '2025-07-20' AND '2025-07-22'
```

ShardingSphere automatically transforms to:
```sql
SELECT * FROM sms_20250720 WHERE created_at BETWEEN '2025-07-20' AND '2025-07-22'
UNION ALL
SELECT * FROM sms_20250721 WHERE created_at BETWEEN '2025-07-20' AND '2025-07-22'
UNION ALL
SELECT * FROM sms_20250722 WHERE created_at BETWEEN '2025-07-20' AND '2025-07-22'
```

## ⚙️ Configuration Options

### @ShardingTable Annotation

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| value | String | Required | Logical table name |
| mode | ShardingMode | MULTI_TABLE | Storage strategy |
| retentionSpanDays | int | 7 | Days to keep data |
| autoManagePartition | boolean | true | Enable auto management |
| partitionAdjustmentTime | String | "04:00" | Daily maintenance time |

### ShardingRepositoryBuilder Options

| Method | Default | Description |
|--------|---------|-------------|
| host() | localhost | MySQL host |
| port() | 3306 | MySQL port |
| maxPoolSize() | 10 | Connection pool size |
| connectionTimeout() | 60000 | Timeout in ms |
| createTablesOnStartup() | true | Create tables on startup |
| charset() | utf8mb4 | Database charset |
| collation() | utf8mb4_unicode_ci | Database collation |

## 📊 Best Practices

### 1. Choose the Right Mode
- **Multi-Table**: High insert rate, simple queries, independent scaling
- **Partitioned Table**: Complex queries, foreign keys, consistent backups

### 2. Retention Policy
- Set `retentionSpanDays` based on your data lifecycle
- Consider storage costs vs query performance
- Account for +N days for future-dated data

### 3. Query Optimization
- Always include date range in WHERE clause for partition pruning
- Use the shard key (created_at) in queries when possible
- For findById(), provide date range when known

### 4. Connection Pool Tuning
```java
.maxPoolSize(20)           // For high concurrency
.connectionTimeout(120000) // 2 minutes for large partition operations
.minIdle(5)               // Maintain minimum connections
```

### 5. Monitoring
```java
// Check partition management status
factory.getPartitionService().printStatus();

// Enable ShardingSphere SQL logging
// Look for "Logic SQL" vs "Actual SQL" in logs
```

## 🔍 Troubleshooting

### Common Issues

1. **"Partition management on a not partitioned table"**
   - Ensure you're using PARTITIONED_TABLE mode for MySQL partitions
   - Multi-table mode doesn't need partition commands

2. **"VALUES LESS THAN value must be strictly increasing"**
   - This is handled automatically by the framework
   - Check if manual partition creation conflicts

3. **Connection timeouts during partition operations**
   - Increase connectionTimeout: `.connectionTimeout(300000)` (5 minutes)

4. **Tables not created on startup**
   - Check `autoManagePartition=true` in entity
   - Check `createTablesOnStartup(true)` in builder
   - Both must be true for automatic creation

## 🤝 Contributing

This is an internal framework. For issues or improvements, contact the platform team.

## 📄 License

Proprietary - Telcobright Ltd.