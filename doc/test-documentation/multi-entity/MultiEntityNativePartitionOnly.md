# MultiEntityNativePartitionOnly Test Documentation

## Test Overview
This test validates the MultiEntityRepository functionality when all registered entities use PARTITIONED mode (native MySQL partitioning with a single table per entity).

## Test Objectives
1. Verify multiple entities can be registered with PARTITIONED mode
2. Test that each entity gets its own partitioned table
3. Validate CRUD operations work correctly across different entities
4. Confirm partition pruning optimization works for date-range queries
5. Ensure entity isolation (operations on one entity don't affect others)

## Test Configuration

### Entities
- **User**: User profiles with registration timestamps
- **Order**: E-commerce orders with order dates
- **AuditLog**: System audit logs with event timestamps

### Repository Configuration
```java
MultiEntityRepository repository = MultiEntityRepository.builder()
    .defaultConnection("127.0.0.1", 3306, "test_db", "root", "password")

    // All entities use PARTITIONED mode
    .registerEntity(User.class)
        .repositoryMode(RepositoryMode.PARTITIONED)
        .partitionRange(PartitionRange.DAILY)
        .retentionDays(30)
        .and()

    .registerEntity(Order.class)
        .repositoryMode(RepositoryMode.PARTITIONED)
        .partitionRange(PartitionRange.MONTHLY)
        .retentionDays(365)
        .and()

    .registerEntity(AuditLog.class)
        .repositoryMode(RepositoryMode.PARTITIONED)
        .partitionRange(PartitionRange.HOURLY)
        .retentionDays(7)
        .and()

    .build();
```

## Test Scenarios

### 1. Entity Registration and Validation
```java
@Test
public void testEntityRegistration() {
    // Verify all entities are registered
    assertTrue(repository.isEntityRegistered(User.class));
    assertTrue(repository.isEntityRegistered(Order.class));
    assertTrue(repository.isEntityRegistered(AuditLog.class));

    // Verify entity info extraction
    EntityInfo userInfo = repository.getEntityInfo(User.class);
    assertEquals("users", userInfo.tableName);
    assertEquals("id", userInfo.idFieldName);
    assertEquals("created_at", userInfo.shardingKeyFieldName);
}
```

### 2. Table Creation Verification
```java
@Test
public void testTableCreation() throws SQLException {
    // Verify each entity has its own partitioned table
    assertTrue(tableExists("users"));
    assertTrue(tableExists("orders"));
    assertTrue(tableExists("audit_logs"));

    // Verify partition structure for each table
    verifyPartitions("users", 61);    // 30 days before + today + 30 days after
    verifyPartitions("orders", 25);   // 12 months before + current + 12 months after
    verifyPartitions("audit_logs", 337); // 7*24 hours before + current + 7*24 hours after
}
```

### 3. Basic CRUD Operations Per Entity
```java
@Test
public void testCrudOperations() throws SQLException {
    // Insert into different entities
    User user = createUser("user1", LocalDateTime.now());
    repository.insert(User.class, user);

    Order order = createOrder("order1", LocalDateTime.now());
    repository.insert(Order.class, order);

    AuditLog log = createAuditLog("log1", LocalDateTime.now());
    repository.insert(AuditLog.class, log);

    // Retrieve from different entities
    User retrievedUser = repository.findById(User.class, "user1");
    assertNotNull(retrievedUser);
    assertEquals("user1", retrievedUser.getId());

    Order retrievedOrder = repository.findById(Order.class, "order1");
    assertNotNull(retrievedOrder);
    assertEquals("order1", retrievedOrder.getId());

    // Update operations
    user.setEmail("updated@example.com");
    repository.updateById(User.class, "user1", user);

    // Delete operations
    repository.deleteById(AuditLog.class, "log1");
    assertNull(repository.findById(AuditLog.class, "log1"));
}
```

### 4. Batch Operations
```java
@Test
public void testBatchOperations() throws SQLException {
    // Insert multiple entities
    List<User> users = createUsers(1000);
    repository.insertMultiple(User.class, users);

    List<Order> orders = createOrders(500);
    repository.insertMultiple(Order.class, orders);

    // Verify counts
    LocalDateTime start = LocalDateTime.now().minusDays(1);
    LocalDateTime end = LocalDateTime.now().plusDays(1);

    long userCount = repository.countByPartitionColBetween(User.class, start, end);
    assertEquals(1000, userCount);

    long orderCount = repository.countByPartitionColBetween(Order.class, start, end);
    assertEquals(500, orderCount);
}
```

### 5. Partition Pruning Verification
```java
@Test
public void testPartitionPruning() throws SQLException {
    // Insert data across multiple partitions
    LocalDateTime today = LocalDate.now().atStartOfDay();

    // Insert users across 7 days
    for (int day = 0; day < 7; day++) {
        LocalDateTime date = today.plusDays(day);
        for (int i = 0; i < 100; i++) {
            User user = createUser("user_" + day + "_" + i, date);
            repository.insert(User.class, user);
        }
    }

    // Query specific date range (should use partition pruning)
    LocalDateTime queryStart = today.plusDays(2);
    LocalDateTime queryEnd = today.plusDays(4);

    List<User> rangeUsers = repository.findByPartitionColBetween(
        User.class, queryStart, queryEnd);

    // Should only get users from days 2-3 (200 users)
    assertEquals(200, rangeUsers.size());

    // Verify with EXPLAIN that partition pruning occurred
    verifyPartitionPruningUsed("users", queryStart, queryEnd);
}
```

### 6. Cross-Entity Isolation
```java
@Test
public void testEntityIsolation() throws SQLException {
    // Operations on one entity shouldn't affect others
    String sharedId = "shared_id_123";

    // Insert entities with same ID (allowed across different tables)
    User user = createUser(sharedId, LocalDateTime.now());
    repository.insert(User.class, user);

    Order order = createOrder(sharedId, LocalDateTime.now());
    repository.insert(Order.class, order);

    // Delete from one entity
    repository.deleteById(User.class, sharedId);

    // Verify other entity is unaffected
    assertNull(repository.findById(User.class, sharedId));
    assertNotNull(repository.findById(Order.class, sharedId));
}
```

### 7. Concurrent Operations
```java
@Test
public void testConcurrentOperations() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(3);
    AtomicInteger errors = new AtomicInteger(0);

    // Concurrent operations on different entities
    executor.submit(() -> {
        try {
            for (int i = 0; i < 1000; i++) {
                User user = createUser("concurrent_user_" + i, LocalDateTime.now());
                repository.insert(User.class, user);
            }
        } catch (Exception e) {
            errors.incrementAndGet();
        } finally {
            latch.countDown();
        }
    });

    executor.submit(() -> {
        try {
            for (int i = 0; i < 1000; i++) {
                Order order = createOrder("concurrent_order_" + i, LocalDateTime.now());
                repository.insert(Order.class, order);
            }
        } catch (Exception e) {
            errors.incrementAndGet();
        } finally {
            latch.countDown();
        }
    });

    executor.submit(() -> {
        try {
            for (int i = 0; i < 1000; i++) {
                AuditLog log = createAuditLog("concurrent_log_" + i, LocalDateTime.now());
                repository.insert(AuditLog.class, log);
            }
        } catch (Exception e) {
            errors.incrementAndGet();
        } finally {
            latch.countDown();
        }
    });

    latch.await(30, TimeUnit.SECONDS);
    assertEquals(0, errors.get());
}
```

### 8. Invalid Entity Handling
```java
@Test(expected = IllegalArgumentException.class)
public void testInvalidEntityAccess() throws SQLException {
    // Try to access non-registered entity
    repository.findById(NonRegisteredEntity.class, "id");
}
```

## Expected Results

### Performance Metrics
- Entity registration: < 100ms per entity
- Single insert: < 10ms
- Batch insert (1000 records): < 500ms
- Query with partition pruning: < 50ms for 1000 records
- Query without partition pruning: > 200ms for same data

### Database State
After test completion:
- 3 partitioned tables created (users, orders, audit_logs)
- Each table has appropriate partition structure
- No cross-contamination between entities
- Partition maintenance scheduled and operational

## SQL Verification Queries

```sql
-- Verify partition structure for users table
SELECT
    PARTITION_NAME,
    PARTITION_EXPRESSION,
    PARTITION_DESCRIPTION
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_NAME = 'users'
  AND TABLE_SCHEMA = 'test_db';

-- Check partition pruning in query plan
EXPLAIN PARTITIONS
SELECT * FROM users
WHERE created_at BETWEEN '2025-01-01' AND '2025-01-02';

-- Verify table isolation
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM audit_logs;
```

## Cleanup
```java
@After
public void cleanup() {
    repository.shutdown();
    // Tables and partitions are retained for analysis
}
```

## Notes
- All entities use PARTITIONED mode but with different partition ranges (DAILY, MONTHLY, HOURLY)
- Each entity maintains its own connection pool through the underlying repository
- Reflection is performed only during registration, not during runtime operations
- Partition pruning significantly improves query performance for date-range queries