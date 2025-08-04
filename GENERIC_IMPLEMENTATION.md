# 🚀 Generic Repository Implementation

This document outlines the new generic repository implementation that automatically infers SQL from entity annotations and uses reflection only once at startup.

## ✨ Key Improvements

### 1. **Automatic SQL Generation from Entities**
- INSERT, SELECT, and CREATE TABLE statements are automatically generated from entity annotations
- No more hardcoded SQL - everything is derived from the entity structure
- Type-safe parameter binding based on field types

### 2. **Reflection Performed Once at Startup**
- All reflection and metadata parsing happens in the repository constructor
- Runtime operations use pre-computed metadata for maximum performance
- Zero reflection overhead during insert/query operations

### 3. **Generic Type Safety**
- Repositories are now generic: `Repository<TEntity, TKey>`
- Example: `GenericMultiTableRepository<SmsEntity, Long>`
- Full compile-time type safety for entities and primary keys

### 4. **Annotation-Based Entity Mapping**
- `@Table(name = "table_name")` - Specifies table name
- `@Id` - Marks primary key field (with auto-generation support)
- `@ShardingKey` - Marks the field used for date-based partitioning
- `@Column` - Configures column mapping with options like nullable, insertable, updatable

## 📊 Entity Example

```java
@Table(name = "sms")
public class SmsEntity {
    
    @Id
    @Column(name = "id", insertable = false, updatable = false)
    private Long id;
    
    @Column(name = "user_id")
    private String userId;
    
    @ShardingKey
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "message")
    private String message;
    
    // ... other fields with getters/setters
}
```

## 🏗️ Repository Usage

### Multi-Table Repository
```java
// Create repository with full type safety
GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
    GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
        .database("messaging")
        .username("root")
        .password("password")
        .tablePrefix("sms")  // Optional: inferred from @Table if not provided
        .partitionRetentionPeriod(30)
        .autoManagePartitions(true)
        .build();

// Insert - SQL automatically generated
SmsEntity sms = new SmsEntity("user123", "+1234567890", "Hello!", "SENT", 
    LocalDateTime.now(), new BigDecimal("0.05"), "twilio");
smsRepo.insert(sms);  // Auto-generates: INSERT INTO messaging.sms_20250804 (user_id, phone_number, message, status, created_at, cost, provider) VALUES (?, ?, ?, ?, ?, ?, ?)

// Find by ID across all tables
SmsEntity found = smsRepo.findById(123L);

// Type-safe queries
List<SmsEntity> recent = smsRepo.findByDateRange(yesterday, now);
```

### Partitioned Table Repository
```java
// Create repository with full type safety
GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = 
    GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
        .database("ecommerce")
        .username("root")
        .password("password")
        .tableName("orders")  // Optional: inferred from @Table if not provided
        .partitionRetentionPeriod(365)
        .autoManagePartitions(true)
        .build();

// Insert - SQL automatically generated with MySQL native partitioning
OrderEntity order = new OrderEntity("CUST001", "ORD-2025-001", 
    new BigDecimal("299.99"), "CONFIRMED", "CREDIT_CARD", 
    "123 Main St", LocalDateTime.now(), 3);
orderRepo.insert(order);  // Auto-generates: INSERT INTO ecommerce.orders (customer_id, order_number, total_amount, status, payment_method, shipping_address, created_at, item_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)

// Find by ID with partition pruning
OrderEntity found = orderRepo.findById(456L);
```

## 🔧 Generated SQL Examples

### Automatically Generated INSERT SQL
```sql
-- For SmsEntity
INSERT INTO messaging.sms_20250804 (user_id, phone_number, message, status, created_at, delivered_at, cost, provider) VALUES (?, ?, ?, ?, ?, ?, ?, ?)

-- For OrderEntity  
INSERT INTO ecommerce.orders (customer_id, order_number, total_amount, status, payment_method, shipping_address, created_at, shipped_at, delivered_at, item_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

### Automatically Generated CREATE TABLE SQL
```sql
-- For SmsEntity (Multi-Table)
CREATE TABLE IF NOT EXISTS messaging.sms_20250804 (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id VARCHAR(255),
  phone_number VARCHAR(255),
  message TEXT,
  status VARCHAR(255),
  created_at DATETIME NOT NULL,
  delivered_at DATETIME,
  cost DECIMAL(10,2),
  provider VARCHAR(255),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

-- For OrderEntity (Partitioned Table)
CREATE TABLE IF NOT EXISTS ecommerce.orders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer_id VARCHAR(255) NOT NULL,
  order_number VARCHAR(255) NOT NULL,
  total_amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(255) NOT NULL,
  payment_method VARCHAR(255),
  shipping_address TEXT,
  created_at DATETIME NOT NULL,
  shipped_at DATETIME,
  delivered_at DATETIME,
  item_count INT,
  KEY idx_created_at (created_at)
) ENGINE=InnoDB
PARTITION BY RANGE (TO_DAYS(created_at))
```

## ⚡ Performance Benefits

### 1. **Zero Runtime Reflection**
- All metadata is cached in `EntityMetadata<T, K>` at startup
- Field access uses pre-computed `Field` references
- SQL generation happens once, cached for reuse

### 2. **Type-Safe Parameter Binding**
- Parameters are bound using the correct JDBC method based on field type
- No boxing/unboxing overhead for primitives
- Proper handling of null values and LocalDateTime conversion

### 3. **Efficient Field Mapping**  
- Direct field access without method reflection
- Snake_case to camelCase conversion cached at startup
- SQL type mapping computed once per field

## 🎯 Migration Guide

### Old Implementation
```java
// Hardcoded for specific entity
MultiTableRepository smsRepo = new MultiTableRepository(config);
smsRepo.insert(smsEntity);  // Hardcoded SQL with manual parameter binding
```

### New Implementation
```java
// Generic with automatic SQL generation
GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
    GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
        .database("messaging")
        .username("root")
        .password("password")
        .build();

smsRepo.insert(smsEntity);  // Automatic SQL generation from annotations
```

## 🔒 Type Safety Benefits

### Compile-Time Guarantees
- Repository method signatures ensure type consistency
- Primary key type is enforced: `findById(K id)` where K is the key type
- Entity type is enforced: `insert(T entity)` where T is the entity type
- No ClassCastException at runtime

### IDE Support
- Full autocompletion for repository methods
- Generic type parameters shown in IDE
- Compile-time error detection for type mismatches

This implementation provides the best of both worlds: **maximum performance** through startup-time reflection and **maximum flexibility** through automatic SQL generation from entity structure.