# Repository Builder Pattern

This document describes the builder pattern for instantiating repository classes with flexible configuration options.

## Overview

Both `MultiTableRepository` and `PartitionedTableRepository` now support a fluent builder pattern for configuration, providing better readability and validation.

## MultiTableRepository Builder

### Basic Usage

```java
MultiTableRepository smsRepo = MultiTableRepository.builder()
    .database("test")
    .username("root")
    .password("password")
    .tablePrefix("sms")
    .build();
```

### Complete Configuration

```java
MultiTableRepository smsRepo = MultiTableRepository.builder()
    .host("localhost")                        // Optional: MySQL host (default: localhost)
    .port(3306)                              // Optional: MySQL port (default: 3306)
    .database("production")                   // Required: Database name
    .username("root")                        // Required: MySQL username
    .password("password")                    // Required: MySQL password
    .tablePrefix("sms")                      // Required: Table prefix (creates sms_YYYYMMDD tables)
    .partitionRetentionPeriod(90)            // Optional: Keep 90 days of data (default: 30)
    .autoManagePartitions(true)              // Optional: Enable automatic cleanup (default: true)
    .initializePartitionsOnStart(true)       // Optional: Create partitions on startup (default: false)
    .build();
```

## PartitionedTableRepository Builder

### Basic Usage

```java
PartitionedTableRepository orderRepo = PartitionedTableRepository.builder()
    .database("ecommerce")
    .username("root")
    .password("password")
    .tableName("orders")
    .build();
```

### Complete Configuration

```java
PartitionedTableRepository orderRepo = PartitionedTableRepository.builder()
    .host("localhost")                       // Optional: MySQL host (default: localhost)
    .port(3306)                             // Optional: MySQL port (default: 3306)
    .database("ecommerce")                  // Required: Database name
    .username("root")                       // Required: MySQL username
    .password("password")                   // Required: MySQL password
    .tableName("orders")                    // Required: Table name (single partitioned table)
    .partitionRetentionPeriod(30)           // Optional: Keep 30 days of data (default: 30)
    .autoManagePartitions(true)             // Optional: Enable automatic cleanup (default: true)
    .initializePartitionsOnStart(false)     // Optional: Create partitions on startup (default: false)
    .build();
```

## Configuration Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `database` | String | Database/schema name |
| `username` | String | MySQL username |
| `password` | String | MySQL password |
| `tablePrefix` / `tableName` | String | Table identifier (prefix for multi-table, name for partitioned) |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | String | localhost | MySQL server host |
| `port` | int | 3306 | MySQL server port |
| `partitionRetentionPeriod` | int | 30 | Number of days to keep data before cleanup |
| `autoManagePartitions` | boolean | true | Enable automatic partition/table cleanup |
| `initializePartitionsOnStart` | boolean | false | Create required partitions during construction |

## Configuration Examples

### High-Volume Short-Term Data (SMS, Events)

```java
MultiTableRepository repo = MultiTableRepository.builder()
    .dataSource(dataSource)
    .database("analytics")
    .tablePrefix("events")
    .partitionRetentionPeriod(7)              // Keep 1 week
    .autoManagePartitions(true)               // Aggressive cleanup
    .initializePartitionsOnStart(true)        // Ready immediately
    .build();
```

### Business Data with Compliance Requirements

```java
PartitionedTableRepository repo = PartitionedTableRepository.builder()
    .dataSource(dataSource)
    .database("finance")
    .tableName("transactions")
    .partitionRetentionPeriod(2555)           // 7 years for compliance
    .autoManagePartitions(false)              // Manual cleanup for audit trail
    .initializePartitionsOnStart(false)       // Manual setup
    .build();
```

### Development/Testing Environment

```java
MultiTableRepository repo = MultiTableRepository.builder()
    .dataSource(dataSource)
    .database("test")
    .tablePrefix("sms_dev")
    .partitionRetentionPeriod(3)              // Keep 3 days only
    .autoManagePartitions(true)               // Clean up frequently
    .initializePartitionsOnStart(false)       // Manual setup for testing
    .build();
```

## Validation

The builder performs validation on `build()`:

- **DataSource**: Must not be null
- **Database**: Must not be null or empty
- **Table identifier**: Must not be null or empty  
- **Retention period**: Must be positive

```java
// This will throw IllegalArgumentException
MultiTableRepository repo = MultiTableRepository.builder()
    .dataSource(null)  // Invalid!
    .database("")      // Invalid!
    .tablePrefix("sms")
    .partitionRetentionPeriod(-1)  // Invalid!
    .build();
```

## Automatic Partition Management

Repositories automatically handle partition/table creation and cleanup based on configuration:

### Automatic Behavior

**During Insert Operations:**
- Tables/partitions are automatically created for the data's date
- If `autoManagePartitions=true`: Future tables/partitions are pre-created
- If `autoManagePartitions=true`: Old tables/partitions are cleaned up based on retention period

**Example:**
```java
MultiTableRepository repo = MultiTableRepository.builder()
    .dataSource(dataSource)
    .database("test")
    .tablePrefix("sms")
    .partitionRetentionPeriod(7)
    .autoManagePartitions(true)
    .build();

// Insert automatically handles table creation and cleanup
SmsEntity sms = new SmsEntity(...);
repo.insert(sms);  // Table sms_YYYYMMDD created automatically
                   // Old tables beyond 7 days removed automatically
```

### Manual Operations (Optional)

For advanced use cases, you can still access manual methods:

```java
// Manual cleanup (respects autoManagePartitions setting)
repo.cleanupOldTables();
repo.cleanupOldPartitions();

// Configuration access
System.out.println("Retention: " + repo.getPartitionRetentionPeriod());
System.out.println("Auto-manage: " + repo.isAutoManagePartitions());
```

## Migration from Constructor

### Before (Constructor)

```java
MultiTableRepository smsRepo = new MultiTableRepository(dataSource, "test", "sms");
PartitionedTableRepository orderRepo = new PartitionedTableRepository(dataSource, "test", "orders");
```

### After (Builder)

```java
MultiTableRepository smsRepo = MultiTableRepository.builder()
    .dataSource(dataSource)
    .database("test")
    .tablePrefix("sms")
    .build();

PartitionedTableRepository orderRepo = PartitionedTableRepository.builder()
    .dataSource(dataSource)
    .database("test")
    .tableName("orders")
    .build();
```

## Benefits

- **Fluent API**: More readable and self-documenting
- **Validation**: Compile-time and runtime parameter checking
- **Flexibility**: Easy to add new configuration options
- **Defaults**: Sensible defaults for optional parameters
- **Immutability**: Repository instances are immutable after construction
- **Testing**: Easy to create repositories with different configurations for testing