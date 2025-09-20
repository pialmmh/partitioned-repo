# Data Operations

## Overview

Split-Verse provides comprehensive data operations across sharded and partitioned datasets. All operations are designed to be transparent to the application while handling the complexity of distributed data management.

## CRUD Operations

### 1. **Insert Operations**

**Single Insert:**
```java
repository.insert(entity);
```

**Internal Flow:**
1. Validate partition boundaries
2. Select target shard (hash-based)
3. Determine target table/partition
4. Execute insert via PreparedStatement
5. Handle any database-specific behavior

**Boundary Validation:**
```java
if (entityDate > maxPartitionDate) {
    throw new SQLException("Beyond maximum partition boundary");
}
if (entityDate < minPartitionDate) {
    // Use minimum partition table (MySQL-style)
    targetTable = minPartitionTable;
}
```

**Bulk Insert:**
```java
repository.insertMultiple(List<T> entities);
```

**Optimization:**
- Group entities by target table
- Use batch PreparedStatements
- Single transaction per table batch
- Configurable batch sizes

### 2. **Read Operations**

**Find by ID:**
```java
T entity = repository.findById(id);
```

**Strategy:**
1. Hash ID to determine shard
2. Query all tables in shard (ID location unknown)
3. Return first match
4. Cache result location (optional)

**Find by ID with Partition Hint:**
```java
T entity = repository.findByIdAndPartitionRange(id, startDate, endDate);
```

**Benefits:**
- Limits table scan to date range
- Improves query performance
- Reduces database load

**Find All in Range:**
```java
List<T> entities = repository.findAllByPartitionRange(startDate, endDate);
```

**Implementation:**
```java
// Identify relevant tables
List<String> tables = getTablesForDateRange(startDate, endDate);
// Query each table with proper boundaries
for (String table : tables) {
    String sql = "SELECT * FROM " + table +
                 " WHERE partition_col >= ? AND partition_col < ?";
    results.addAll(executeQuery(sql, startDate, endDate));
}
```

### 3. **Update Operations**

**Update by ID:**
```java
repository.updateById(id, entity);
```

**Challenge:** ID location unknown
**Solution:** Query all tables to find entity first

**Update with Partition Hint:**
```java
repository.updateByIdAndPartitionRange(id, entity, startDate, endDate);
```

**Benefits:**
- Faster location of entity
- Reduced table scans
- Better performance

### 4. **Delete Operations**

**Delete by ID:**
```java
repository.deleteById(id);
```

**Implementation:**
- Attempts delete on all tables
- No error if not found
- Returns success always

**Delete by Range:**
```java
repository.deleteAllByPartitionRange(startDate, endDate);
```

**Batch Delete:**
```sql
DELETE FROM table WHERE partition_col >= ? AND partition_col < ?
```

## Query Patterns

### 1. **Time-Range Queries**

**Efficient Pattern:**
```java
// Good: Includes partition column
repository.findAllByPartitionRange(start, end);

// Less Efficient: No partition hint
repository.findAll(); // Scans all tables
```

**Optimization Techniques:**
- Partition pruning via date ranges
- Parallel table queries
- Result streaming
- Early termination

### 2. **Cross-Shard Queries**

**Fan-Out Pattern:**
```java
public List<T> findAcrossShards(Predicate<T> filter) {
    List<Future<List<T>>> futures = new ArrayList<>();

    // Submit parallel queries to all shards
    for (ShardConfig shard : allShards) {
        futures.add(executor.submit(() ->
            shard.getRepository().findAll()
                .stream()
                .filter(filter)
                .collect(toList())
        ));
    }

    // Aggregate results
    return futures.stream()
        .map(this::getFutureResult)
        .flatMap(List::stream)
        .collect(toList());
}
```

### 3. **Aggregation Queries**

**Count Operations:**
```java
public long countInDateRange(LocalDateTime start, LocalDateTime end) {
    List<String> tables = getTablesForDateRange(start, end);
    long total = 0;

    for (String table : tables) {
        String sql = "SELECT COUNT(*) FROM " + table +
                    " WHERE date_col BETWEEN ? AND ?";
        total += executeCount(sql, start, end);
    }

    return total;
}
```

**Sum/Average/Min/Max:**
- Similar pattern to count
- Aggregate at application level
- Consider materialized views for performance

## Pagination

### 1. **Single-Table Pagination**

```java
public Page<T> findPage(int pageNumber, int pageSize) {
    int offset = pageNumber * pageSize;
    String sql = "SELECT * FROM table LIMIT ? OFFSET ?";

    List<T> content = executeQuery(sql, pageSize, offset);
    long totalElements = executeCount("SELECT COUNT(*) FROM table");

    return new Page<>(content, pageNumber, pageSize, totalElements);
}
```

### 2. **Cross-Table Pagination**

**Challenge:** Maintaining order across tables

**Solution:**
```java
public Page<T> findPageAcrossTables(int pageNumber, int pageSize) {
    // Collect all data with ordering
    TreeSet<T> sorted = new TreeSet<>(comparator);

    for (String table : getAllTables()) {
        sorted.addAll(queryTable(table));
    }

    // Apply pagination
    List<T> page = sorted.stream()
        .skip(pageNumber * pageSize)
        .limit(pageSize)
        .collect(toList());

    return new Page<>(page, pageNumber, pageSize, sorted.size());
}
```

### 3. **Cursor-Based Pagination**

**More Efficient for Large Datasets:**
```java
public List<T> findNextBatch(String lastId, int batchSize) {
    return repository.findBatchByIdGreaterThan(lastId, batchSize);
}
```

**Implementation:**
```sql
SELECT * FROM table
WHERE id > ?
ORDER BY id
LIMIT ?
```

## Transaction Management

### 1. **Single-Shard Transactions**

```java
public void transactionalOperation(T entity1, T entity2) throws SQLException {
    Connection conn = connectionProvider.getConnection();
    try {
        conn.setAutoCommit(false);

        // Both operations on same shard
        repository.insert(entity1);
        repository.update(entity2);

        conn.commit();
    } catch (Exception e) {
        conn.rollback();
        throw e;
    } finally {
        conn.setAutoCommit(true);
        conn.close();
    }
}
```

### 2. **Cross-Shard Operations**

**No Distributed Transactions:**
```java
public void crossShardOperation(T entity1, T entity2) {
    try {
        // Shard 1 operation
        shard1Repository.insert(entity1);

        // Shard 2 operation (may fail)
        shard2Repository.insert(entity2);
    } catch (Exception e) {
        // Manual compensation
        compensate(entity1, entity2);
    }
}
```

### 3. **Saga Pattern**

```java
public class OrderSaga {
    public void executeOrder(Order order) {
        List<SagaStep> steps = Arrays.asList(
            () -> reserveInventory(order),
            () -> chargePayment(order),
            () -> createShipment(order)
        );

        List<CompensationStep> compensations = new ArrayList<>();

        for (SagaStep step : steps) {
            try {
                CompensationStep compensation = step.execute();
                compensations.add(compensation);
            } catch (Exception e) {
                // Rollback in reverse order
                rollback(compensations);
                throw e;
            }
        }
    }
}
```

## Batch Operations

### 1. **Batch Insert**

```java
// Optimal batch size: 100-1000 records
public void batchInsert(List<T> entities) {
    Map<String, List<T>> byTable = groupByTable(entities);

    for (Entry<String, List<T>> entry : byTable.entrySet()) {
        String sql = buildBatchInsertSQL(entry.getKey());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (T entity : entry.getValue()) {
                addBatchParameters(stmt, entity);
            }
            stmt.executeBatch();
        }
    }
}
```

### 2. **Batch Update**

```java
public void batchUpdate(Map<String, T> updates) {
    String sql = "UPDATE table SET field = ? WHERE id = ?";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        for (Entry<String, T> entry : updates.entrySet()) {
            stmt.setObject(1, entry.getValue().getField());
            stmt.setString(2, entry.getKey());
            stmt.addBatch();
        }
        stmt.executeBatch();
    }
}
```

### 3. **Batch Delete**

```java
public void batchDelete(List<String> ids) {
    String sql = "DELETE FROM table WHERE id IN (" +
                 String.join(",", Collections.nCopies(ids.size(), "?")) + ")";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        for (int i = 0; i < ids.size(); i++) {
            stmt.setString(i + 1, ids.get(i));
        }
        stmt.executeUpdate();
    }
}
```

## Query Optimization

### 1. **Index Usage**

**Automatic Indexes:**
```sql
-- Created automatically
CREATE INDEX idx_id ON table (id);
CREATE INDEX idx_partition_col ON table (partition_col);

-- Composite for common queries
CREATE INDEX idx_id_partition ON table (id, partition_col);
```

**Query Hints:**
```sql
-- MySQL
SELECT * FROM table USE INDEX (idx_partition_col) WHERE ...

-- PostgreSQL
SELECT /*+ IndexScan(table idx_partition_col) */ * FROM table WHERE ...
```

### 2. **Partition Pruning**

```java
// Efficient: Partition pruning
SELECT * FROM events_20250920
WHERE event_time >= '2025-09-20 10:00:00'
AND event_time < '2025-09-20 11:00:00'

// Inefficient: Full table scan
SELECT * FROM events_20250920
WHERE HOUR(event_time) = 10
```

### 3. **Query Caching**

```java
public class QueryCache {
    private final Cache<String, List<T>> cache =
        CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();

    public List<T> findWithCache(String query, Object... params) {
        String key = buildCacheKey(query, params);

        return cache.get(key, () ->
            executeQuery(query, params)
        );
    }
}
```

## Performance Considerations

### 1. **Query Performance**

| Operation | Best Case | Worst Case | Optimization |
|-----------|-----------|------------|--------------|
| Insert | O(1) | O(1) | Direct routing |
| Find by ID | O(1) | O(n) tables | Add partition hint |
| Range Query | O(log n) | O(n) | Partition pruning |
| Full Scan | O(n) | O(n*m) | Avoid if possible |

### 2. **Memory Management**

**Streaming Results:**
```java
public void processLargeDataset(LocalDateTime start, LocalDateTime end) {
    try (Stream<T> stream = repository.streamByDateRange(start, end)) {
        stream.forEach(this::processEntity);
    }
}
```

**Batch Processing:**
```java
String lastId = null;
int batchSize = 1000;

while (true) {
    List<T> batch = repository.findBatchAfterId(lastId, batchSize);
    if (batch.isEmpty()) break;

    processBatch(batch);
    lastId = batch.get(batch.size() - 1).getId();
}
```

### 3. **Connection Management**

**Connection Pooling:**
```java
// HikariCP configuration
HikariConfig config = new HikariConfig();
config.setMaximumPoolSize(20);
config.setMinimumIdle(5);
config.setConnectionTimeout(30000);
config.setIdleTimeout(600000);
```

**Connection per Operation:**
- Each operation gets fresh connection
- Auto-closed after operation
- No connection holding across operations

## Error Handling

### 1. **Retry Logic**

```java
public T executeWithRetry(Supplier<T> operation, int maxRetries) {
    int attempts = 0;
    while (attempts < maxRetries) {
        try {
            return operation.get();
        } catch (SQLException e) {
            if (!isRetryable(e) || ++attempts >= maxRetries) {
                throw e;
            }
            waitWithBackoff(attempts);
        }
    }
}
```

### 2. **Partial Failures**

```java
public BatchResult batchOperationWithTracking(List<T> entities) {
    List<T> succeeded = new ArrayList<>();
    List<T> failed = new ArrayList<>();

    for (T entity : entities) {
        try {
            repository.insert(entity);
            succeeded.add(entity);
        } catch (Exception e) {
            failed.add(entity);
            log.error("Failed to insert: " + entity.getId(), e);
        }
    }

    return new BatchResult(succeeded, failed);
}
```

### 3. **Deadlock Handling**

```java
public void handleDeadlock(Runnable operation) {
    try {
        operation.run();
    } catch (SQLException e) {
        if (isDeadlock(e)) {
            // Retry with random delay
            Thread.sleep(random.nextInt(1000));
            operation.run();
        } else {
            throw e;
        }
    }
}