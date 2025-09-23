# Sequential Log Repository

## Overview
A simplified repository builder specifically designed for log preservation with automatic partition rotation. This utility leverages the existing `GenericPartitionedTableRepository` with optimized settings for sequential log storage.

## Key Features

### 1. Date-Based Partitioning
- Uses LocalDateTime as the sharding key
- Supports daily partitions by default (MySQL RANGE partitioning)
- Automatic partition creation for future dates
- Automatic dropping of old partitions beyond retention period

### 2. Sequential ID Support
- Designed for sequential ID-based iteration (`findBatchByIdGreaterThan`)
- Includes utility class for generating time-based sequential IDs
- IDs are naturally sortable and work with string comparison

### 3. Minimal Configuration
- Simple builder pattern with sensible defaults
- 30-day retention by default
- Automatic maintenance at 2 AM daily
- Binary collation for efficient sequential ID comparison

## Usage

### Basic Setup
```java
ShardingRepository<LogEntry, LocalDateTime> repository =
    SequentialLogRepositoryBuilder.create(LogEntry.class)
        .connection("localhost", 3306, "logs_db", "user", "pass")
        .tableName("app_logs")
        .retentionDays(30)
        .build();
```

### Entity Requirements
```java
@Table(name = "logs")
public class LogEntry implements ShardingEntity<LocalDateTime> {

    @Id
    @Column(name = "id")
    private String id;

    @ShardingKey  // Date-based partitioning key
    @Column(name = "timestamp")
    private LocalDateTime timestamp;

    // Other fields...

    @Override
    public String getId() { return id; }

    @Override
    public void setId(String id) { this.id = id; }

    @Override
    public LocalDateTime getPartitionColValue() { return timestamp; }

    @Override
    public void setPartitionColValue(LocalDateTime value) {
        this.timestamp = value;
    }
}
```

### Sequential ID Generation
```java
// Generate time-based sequential ID
String id = SequentialLogRepositoryBuilder.SequentialIdGenerator.generate();
// Format: "1737562800000-000001"

// With custom prefix
String id = SequentialIdGenerator.generate("app1");
// Format: "app1-1737562800000-000001"
```

### Log Iteration Pattern
```java
// Iterate through all logs sequentially
String cursor = null;
int batchSize = 1000;

while (true) {
    List<LogEntry> batch = repository.findBatchByIdGreaterThan(cursor, batchSize);

    if (batch.isEmpty()) {
        break;
    }

    // Process batch
    processBatch(batch);

    // Update cursor for next iteration
    cursor = batch.get(batch.size() - 1).getId();
}
```

### Date Range Queries
```java
// Find logs within date range
LocalDateTime startDate = LocalDateTime.now().minusDays(1);
LocalDateTime endDate = LocalDateTime.now();

List<LogEntry> logs = repository.findAllByPartitionRange(startDate, endDate);
```

## Use Cases

1. **Application Logs**: High-volume application logging with automatic cleanup
2. **Audit Trails**: Compliance-focused audit logging with retention policies
3. **Event Streams**: Time-series event data with sequential processing
4. **Metrics/Telemetry**: Performance metrics with time-based partitioning

## Architecture Notes

### Partition Structure
```sql
-- Table: logs
-- Partition p20250920: 2025-09-20 00:00:00 to 2025-09-20 23:59:59
-- Partition p20250921: 2025-09-21 00:00:00 to 2025-09-21 23:59:59
-- ...

CREATE TABLE logs (
    id VARCHAR(64) PRIMARY KEY,
    timestamp DATETIME(6),
    -- other columns
    KEY idx_timestamp (timestamp)
) PARTITION BY RANGE(TO_DAYS(timestamp)) (
    PARTITION p20250920 VALUES LESS THAN (TO_DAYS('2025-09-21')),
    PARTITION p20250921 VALUES LESS THAN (TO_DAYS('2025-09-22')),
    -- ...
);
```

### Maintenance Process
- Runs daily at configured time (default 2 AM)
- Drops partitions older than retention period
- Creates new partitions for upcoming days
- Zero-downtime operation

### Performance Considerations
1. **Partition Pruning**: Queries with date ranges automatically skip irrelevant partitions
2. **Sequential Access**: Optimized for cursor-based iteration
3. **Bulk Inserts**: Supports batch insertion for high throughput
4. **Index Strategy**: Primary key on ID, index on timestamp for range queries

## Limitations

1. **Single Table**: Uses native MySQL partitioning, not multi-table approach
2. **Daily Partitions Only**: Currently supports daily partitions (can be extended)
3. **No Updates**: Logs are immutable - updates not recommended
4. **LocalDateTime Only**: Requires LocalDateTime as partition key

## Migration from Flat Table

```sql
-- Step 1: Create partitioned table
CREATE TABLE logs_new LIKE logs;
ALTER TABLE logs_new PARTITION BY RANGE(TO_DAYS(timestamp)) (...);

-- Step 2: Copy data
INSERT INTO logs_new SELECT * FROM logs;

-- Step 3: Swap tables
RENAME TABLE logs TO logs_old, logs_new TO logs;

-- Step 4: Drop old table
DROP TABLE logs_old;
```

## Best Practices

1. **ID Generation**: Use provided SequentialIdGenerator or similar time-based IDs
2. **Batch Processing**: Process logs in batches using cursor-based pagination
3. **Retention Policy**: Set appropriate retention period based on compliance needs
4. **Monitoring**: Monitor partition count and sizes
5. **Indexes**: Add indexes on frequently queried columns besides timestamp