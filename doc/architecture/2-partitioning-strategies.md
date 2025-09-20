# Partitioning Strategies

## Overview

Split-Verse implements a multi-level partitioning strategy that combines horizontal sharding across databases with table-level partitioning within each shard. This approach enables massive scalability while maintaining query performance.

## Partitioning Hierarchy

```
Level 1: Database Sharding (Horizontal)
    ↓
Level 2: Table Partitioning (Repository Mode)
    ↓
Level 3: Native Partitions (Optional)
```

## Repository Modes

### 1. **MULTI_TABLE Mode**
Creates separate physical tables for each partition period.

**Structure:**
```
database_shard_1/
├── events_20250915  (September 15 data)
├── events_20250916  (September 16 data)
├── events_20250917  (September 17 data)
└── events_20250918  (September 18 data)
```

**Characteristics:**
- One table per time period (day/hour/month/year)
- Simple table structure
- Easy maintenance (DROP old tables)
- No partition overhead
- Best for time-series data with clear retention policies

**Use Cases:**
- Log data with daily retention
- Metrics with hourly granularity
- Events with monthly archival

### 2. **PARTITIONED Mode**
Single table with native database partitions.

**Structure:**
```
database_shard_1/
└── events (partitioned table)
    ├── p2025_09_15  (Partition for Sept 15)
    ├── p2025_09_16  (Partition for Sept 16)
    ├── p2025_09_17  (Partition for Sept 17)
    └── p2025_09_18  (Partition for Sept 18)
```

**Characteristics:**
- Single logical table
- Native partition pruning
- Complex maintenance (ALTER TABLE)
- Better for smaller partition counts
- Requires database partition support

**Use Cases:**
- Reference data with range queries
- Moderate data volume
- Need for partition-aware operations

## Sharding Strategy

### **Hash-Based Sharding**
Default strategy using consistent hashing on entity ID.

```java
// Shard selection algorithm
int shardIndex = Math.abs(entityId.hashCode()) % totalShards;
ShardConfig targetShard = shards.get(shardIndex);
```

**Benefits:**
- Even data distribution
- Predictable routing
- No hotspots
- Simple implementation

**Limitations:**
- No range queries across shards
- Rebalancing requires data migration

## Partition Ranges

### Time-Based Partitions

| Range | Table Granularity | Use Case |
|-------|------------------|----------|
| HOURLY | One table/hour | High-frequency data, short retention |
| DAILY | One table/day | Standard logging, 30-90 day retention |
| MONTHLY | One table/month | Reporting data, yearly retention |
| YEARLY | One table/year | Archive data, long-term storage |

### Value-Based Partitions

| Range | Bucket Size | Use Case |
|-------|------------|----------|
| VALUE_RANGE_1K | 1,000 records | Small datasets |
| VALUE_RANGE_10K | 10,000 records | Medium datasets |
| VALUE_RANGE_100K | 100,000 records | Large datasets |
| VALUE_RANGE_1M | 1,000,000 records | Very large datasets |

### Hash-Based Partitions

| Range | Buckets | Use Case |
|-------|---------|----------|
| HASH_BUCKET_16 | 16 tables | Small cardinality |
| HASH_BUCKET_64 | 64 tables | Medium cardinality |
| HASH_BUCKET_256 | 256 tables | Large cardinality |

## Table Naming Conventions

### MULTI_TABLE Mode

**Daily Tables:**
```
{database}.{prefix}_{yyyyMMdd}
Example: mydb.events_20250920
```

**Hourly Tables:**
```
{database}.{prefix}_{yyyyMMddHH}
Example: mydb.events_2025092014
```

**Monthly Tables:**
```
{database}.{prefix}_{yyyyMM}
Example: mydb.events_202509
```

### PARTITIONED Mode

**Main Table:**
```
{database}.{prefix}
Example: mydb.events
```

**Partition Names:**
```
p{yyyy}_{MM}_{dd}_{HH}
Example: p2025_09_20_14
```

## Partition Management

### 1. **Automatic Table Creation**

**Initialization:**
```java
// Tables created on startup based on retention period
startDate = now - retentionDays
endDate = now + retentionDays
createTablesForDateRange(startDate, endDate)
```

**Dynamic Creation:**
- Tables created on-demand for inserts
- Lazy creation for non-time-based partitions

### 2. **Retention Management**

**Automatic Cleanup:**
```java
// Scheduled maintenance task
if (autoManagePartitions) {
    dropTablesOlderThan(retentionPeriod);
    createFutureTables(lookaheadPeriod);
}
```

**Manual Cleanup:**
- Direct DROP TABLE commands
- Batch deletion scripts
- Archive before deletion

### 3. **Boundary Handling**

**MySQL-Style Behavior:**
```java
if (partitionValue < minPartition) {
    // Insert into minimum partition (like MySQL)
    useTable(minPartitionTable);
} else if (partitionValue > maxPartition) {
    // Throw exception (like MySQL)
    throw new SQLException("Value beyond partition boundary");
}
```

## Query Routing

### 1. **Single Entity Queries**
```java
// Direct routing to specific shard and partition
Shard shard = selectShard(entity.getId());
Table table = selectTable(entity.getPartitionValue());
return shard.query(table, entity.getId());
```

### 2. **Range Queries**
```java
// Identify affected partitions
List<Table> tables = getTablesForDateRange(start, end);
// Query each table
for (Table table : tables) {
    results.addAll(queryTable(table, start, end));
}
```

### 3. **Cross-Shard Queries**
```java
// Fan-out to all shards
List<Future<List<T>>> futures = new ArrayList<>();
for (Shard shard : allShards) {
    futures.add(executor.submit(() ->
        shard.findByDateRange(start, end)
    ));
}
// Aggregate results
return aggregateResults(futures);
```

## Optimization Techniques

### 1. **Partition Pruning**
- Use date ranges to limit table scans
- Skip tables outside query range
- Leverage database optimizer

### 2. **Parallel Queries**
- Query multiple partitions concurrently
- Aggregate results in application
- Use thread pool for parallelism

### 3. **Index Strategy**
```sql
-- Automatic indexes per table
CREATE INDEX idx_sharding_key ON table (sharding_column);
CREATE INDEX idx_id ON table (id);

-- Custom indexes via @Index annotation
CREATE INDEX idx_custom ON table (column) WHERE condition;
```

## Best Practices

### 1. **Choosing Repository Mode**

**Use MULTI_TABLE when:**
- Clear time-based retention
- Need simple maintenance
- High insert volume
- Minimal cross-partition queries

**Use PARTITIONED when:**
- Complex partitioning rules
- Need partition-level operations
- Moderate table count
- Database supports partitions well

### 2. **Partition Sizing**

**Optimal Sizes:**
- Daily: 1-100 GB per table
- Hourly: 100 MB - 5 GB per table
- Monthly: 10-500 GB per table

**Factors to Consider:**
- Query patterns
- Maintenance windows
- Backup strategy
- Storage constraints

### 3. **Shard Count Planning**

**Initial Sharding:**
```
Shards = (Expected_Data_Volume / Max_Shard_Size) * Growth_Factor
```

**Growth Strategy:**
- Start with 2-4 shards
- Double when reaching 70% capacity
- Plan for 3-5 year growth

## Migration Strategies

### 1. **Adding New Shards**
```java
// 1. Add new shard configuration
// 2. Run rebalancing job
// 3. Update routing algorithm
// 4. Verify data distribution
```

### 2. **Changing Partition Strategy**
```java
// 1. Create new tables with new strategy
// 2. Migrate data in batches
// 3. Switch application to new tables
// 4. Drop old tables after verification
```

### 3. **Retention Changes**
```java
// Extending retention
createHistoricalTables(oldestDate, newOldestDate);

// Reducing retention
dropTablesOlderThan(newRetentionPeriod);
```

## Performance Characteristics

### MULTI_TABLE Mode

| Operation | Performance | Notes |
|-----------|------------|-------|
| Insert | O(1) | Direct table routing |
| Query by ID | O(1) | If partition known |
| Range Query | O(n) | n = number of tables |
| Maintenance | O(1) | Simple DROP TABLE |

### PARTITIONED Mode

| Operation | Performance | Notes |
|-----------|------------|-------|
| Insert | O(1) | Partition routing |
| Query by ID | O(1) | Partition pruning |
| Range Query | O(log n) | Partition pruning |
| Maintenance | O(n) | ALTER TABLE operations |

## Limitations and Considerations

### 1. **Maximum Partitions**
- MySQL: ~8000 partitions per table
- PostgreSQL: No hard limit (performance degrades)
- Oracle: 1M partitions (with intervals)
- SQL Server: 15,000 partitions

### 2. **Metadata Overhead**
- Each partition has metadata cost
- Information schema queries slow with many partitions
- Consider metadata caching

### 3. **Cross-Partition Operations**
- No foreign keys across partitions
- JOIN operations limited
- Aggregations require application logic