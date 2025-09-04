# Sharding Repository Integration Test Plan

## Test Environment Setup

### Prerequisites
- MySQL 5.7+ or 8.0 running on 127.0.0.1:3306
- Database user: root with password: 123456
- Test database: `test` (will be created/dropped during tests)
- Prometheus/Grafana for monitoring validation (optional)
- At least 10GB free disk space for performance tests

### Initial Database Preparation
```sql
-- Create test database
CREATE DATABASE IF NOT EXISTS test;
CREATE DATABASE IF NOT EXISTS test_backup;
USE test;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON test.* TO 'root'@'127.0.0.1';
GRANT ALL PRIVILEGES ON test_backup.* TO 'root'@'127.0.0.1';
FLUSH PRIVILEGES;
```

---

## Test Scenarios

## 1. MULTI-TABLE REPOSITORY TESTS

### Test 1.1: Table Auto-Creation and Data Distribution
**Objective:** Verify automatic table creation and correct data routing based on timestamp

**Steps:**
1. Start with empty `test` database
2. Create SMS repository with table prefix "sms"
3. Insert 100 SMS entities with timestamps spanning 5 different days:
   - 20 records for today
   - 20 records for yesterday 
   - 20 records for 2 days ago
   - 20 records for 3 days ago
   - 20 records for 4 days ago
4. Verify in MySQL:
   ```sql
   SHOW TABLES LIKE 'sms_%';
   -- Expected: 5 tables (sms_20250104, sms_20250103, etc.)
   
   SELECT table_name, table_rows 
   FROM information_schema.tables 
   WHERE table_schema='test' AND table_name LIKE 'sms_%';
   -- Expected: Each table has ~20 rows
   ```
5. Check hourly partitions within each daily table:
   ```sql
   SELECT partition_name, table_rows 
   FROM information_schema.partitions 
   WHERE table_schema='test' AND table_name='sms_20250104';
   -- Expected: 24 partitions (h00-h23)
   ```

**Success Criteria:**
- [ ] 5 tables created automatically
- [ ] Each table contains exactly the records for that day
- [ ] Each table has 24 hour partitions
- [ ] No data loss or duplication

### Test 1.2: Cross-Table Query Operations
**Objective:** Verify queries correctly span multiple tables

**Steps:**
1. Using repository from Test 1.1
2. Execute findAllByDateRange for entire 5-day period
3. Verify count matches total inserted (100 records)
4. Execute findById for IDs from different days
5. Execute findOneByIdGreaterThan starting from 0
6. Execute findBatchByIdGreaterThan with batch size 25
7. Cross-verify with direct SQL:
   ```sql
   SELECT COUNT(*) FROM sms_20250104 
   UNION ALL 
   SELECT COUNT(*) FROM sms_20250103
   -- ... for all tables
   ```

**Success Criteria:**
- [ ] Date range query returns all 100 records
- [ ] findById successfully retrieves records from any table
- [ ] Cursor iteration works across table boundaries
- [ ] Batch fetching accumulates correctly across tables

### Test 1.3: Automatic Table Maintenance
**Objective:** Verify old table cleanup and future table pre-creation

**Steps:**
1. Configure retention period = 3 days
2. Insert data spanning 10 days (5 past, today, 4 future)
3. Wait for maintenance cycle (or trigger manually)
4. Check tables after maintenance:
   ```sql
   SHOW TABLES LIKE 'sms_%';
   SELECT table_name, create_time 
   FROM information_schema.tables 
   WHERE table_schema='test' AND table_name LIKE 'sms_%'
   ORDER BY table_name;
   ```
5. Verify old data is deleted (older than retention)
6. Verify future tables are pre-created

**Success Criteria:**
- [ ] Tables older than retention period are dropped
- [ ] Current and future tables within retention window exist
- [ ] No data loss for data within retention period
- [ ] Maintenance logs show correct operations

---

## 2. PARTITIONED TABLE REPOSITORY TESTS

### Test 2.1: Partition Creation and Management
**Objective:** Verify MySQL native partitioning works correctly

**Steps:**
1. Create Order repository with table name "orders"
2. Insert 100 orders across 10 different days
3. Check partition structure:
   ```sql
   SELECT 
     partition_name,
     partition_expression,
     partition_description,
     table_rows
   FROM information_schema.partitions
   WHERE table_schema='test' AND table_name='orders'
   ORDER BY partition_ordinal_position;
   ```
4. Verify each day has its own partition
5. Insert order with timestamp 30 days in future
6. Verify new partition auto-created

**Success Criteria:**
- [ ] Single table with multiple partitions
- [ ] Each partition contains correct day's data
- [ ] New partitions created automatically
- [ ] Partition pruning works for date-range queries

### Test 2.2: Partition Pruning Performance
**Objective:** Verify MySQL uses partition pruning for queries

**Steps:**
1. Insert 10,000 orders across 30 days
2. Enable query profiling:
   ```sql
   SET profiling = 1;
   ```
3. Execute date range query via repository
4. Check query execution plan:
   ```sql
   EXPLAIN PARTITIONS 
   SELECT * FROM orders 
   WHERE created_at BETWEEN '2025-01-01' AND '2025-01-02';
   
   SHOW PROFILE;
   ```
5. Verify only relevant partitions are scanned

**Success Criteria:**
- [ ] EXPLAIN shows partition pruning
- [ ] Query time proportional to date range
- [ ] Full table scans avoided for date queries

---

## 3. DATA INTEGRITY TESTS

### Test 3.1: Concurrent Insert Operations
**Objective:** Verify thread safety and data integrity under concurrent load

**Steps:**
1. Create 10 threads/processes
2. Each thread inserts 1000 records simultaneously
3. Use unique ID ranges per thread to track
4. After completion, verify:
   ```sql
   SELECT COUNT(*), COUNT(DISTINCT id) FROM <table>;
   -- Both should be 10,000
   
   SELECT MIN(id), MAX(id), COUNT(*) 
   FROM <table>
   GROUP BY FLOOR(id/1000);
   -- Each group should have exactly 1000
   ```

**Success Criteria:**
- [ ] All 10,000 records inserted
- [ ] No duplicate IDs
- [ ] No missing records
- [ ] No deadlocks or transaction failures

### Test 3.2: Transaction Boundaries
**Objective:** Verify ACID properties maintained

**Steps:**
1. Start transaction
2. Insert 100 records via insertMultiple
3. Kill application mid-transaction
4. Restart and check data:
   ```sql
   SELECT COUNT(*) FROM <table> WHERE batch_id = 'test_batch_1';
   ```
5. Retry same batch insert
6. Verify no duplicates

**Success Criteria:**
- [ ] Either all or none of batch inserted
- [ ] No partial batches
- [ ] Retry handling works correctly

---

## 4. PERFORMANCE AND STRESS TESTS

### Test 4.1: High Volume Insert Performance
**Objective:** Measure insert throughput and identify bottlenecks

**Test Matrix:**
| Batch Size | Record Count | Expected Time | Target TPS |
|------------|-------------|---------------|------------|
| 1          | 10,000      | < 60 sec      | > 166      |
| 100        | 10,000      | < 10 sec      | > 1,000    |
| 1000       | 100,000     | < 30 sec      | > 3,333    |
| 5000       | 500,000     | < 120 sec     | > 4,166    |

**Measurements:**
- Record insert time per batch
- Monitor MySQL connections: `SHOW PROCESSLIST`
- Check disk I/O: `iostat -x 1`
- Monitor table locks: `SHOW ENGINE INNODB STATUS`

### Test 4.2: Query Performance Under Load
**Objective:** Verify query performance with large datasets

**Setup:**
- Pre-populate 1 million records across 30 days

**Test Queries:**
1. Find by ID (should use index): < 10ms
2. Find by date range (1 day): < 100ms  
3. Find by date range (7 days): < 500ms
4. Cursor iteration (batch 1000): < 50ms per batch
5. Concurrent queries (10 threads): No degradation > 20%

**Monitoring:**
```sql
SHOW STATUS LIKE 'Slow_queries';
SHOW STATUS LIKE 'Handler%';
```

### Test 4.3: Long-Running Stability Test
**Objective:** Verify system stability over extended period

**Duration:** 24 hours continuous operation

**Workload:**
- Insert: 100 records/minute
- Query: 10 queries/minute (mixed types)
- Update: 10 updates/minute
- Maintenance: Every hour

**Monitoring Points (every hour):**
- Memory usage (should stabilize)
- Connection pool status
- Table/partition count
- Error rate (should be 0)
- Query response time (P95 < 100ms)

---

## 5. FAILURE RECOVERY TESTS

### Test 5.1: Database Connection Failure
**Objective:** Verify graceful handling of connection loss

**Steps:**
1. Start continuous insert operation
2. Stop MySQL: `systemctl stop mysql`
3. Verify application logs show connection errors
4. Start MySQL: `systemctl start mysql`
5. Verify automatic reconnection
6. Verify no data corruption

**Success Criteria:**
- [ ] Clear error messages in logs
- [ ] Automatic reconnection works
- [ ] No partial writes
- [ ] Operations resume correctly

### Test 5.2: Disk Space Exhaustion
**Objective:** Verify behavior when disk fills up

**Steps:**
1. Fill disk to 95% capacity
2. Attempt large batch insert
3. Monitor error handling
4. Free disk space
5. Retry same operation

**Success Criteria:**
- [ ] Clear "disk full" errors
- [ ] No corruption of existing data
- [ ] Successful retry after space freed

---

## 6. MIGRATION AND COMPATIBILITY TESTS

### Test 6.1: Cross-Strategy Migration
**Objective:** Verify data migration between repository types

**Steps:**
1. Create multi-table repo with 100K records
2. Export data using findAllByDateRange
3. Create partitioned table repo
4. Import data using insertMultiple
5. Verify data integrity:
   ```sql
   -- Compare counts
   SELECT COUNT(*), SUM(id), AVG(id) FROM source_tables;
   SELECT COUNT(*), SUM(id), AVG(id) FROM target_table;
   ```

**Success Criteria:**
- [ ] All records migrated
- [ ] No data modification
- [ ] Timestamps preserved
- [ ] IDs preserved

### Test 6.2: Version Compatibility
**Objective:** Verify compatibility with different MySQL versions

**Test Matrix:**
| MySQL Version | Expected Result |
|--------------|-----------------|
| 5.7.x        | Full support    |
| 8.0.x        | Full support    |
| MariaDB 10.x | Verify partitioning syntax |

---

## 7. MONITORING AND OBSERVABILITY TESTS

### Test 7.1: Metrics Collection Accuracy
**Objective:** Verify monitoring metrics are accurate

**Steps:**
1. Enable monitoring with 1-minute interval
2. Perform exactly 100 inserts, 50 queries, 25 updates
3. Wait for next metric collection
4. Verify metrics match operations:
   - Insert count = 100
   - Query count = 50  
   - Update count = 25
   - Error count = 0

### Test 7.2: Prometheus Integration
**Objective:** Verify metrics exported correctly

**Steps:**
1. Configure Prometheus endpoint
2. Perform operations
3. Query Prometheus:
   ```
   repository_operations_total{type="insert"}
   repository_operations_total{type="query"}
   repository_maintenance_runs_total
   ```
4. Verify Grafana dashboards update

---

## 8. EDGE CASES AND BOUNDARY TESTS

### Test 8.1: Timezone Boundary Handling
**Objective:** Verify correct handling at day boundaries

**Steps:**
1. Insert records at 23:59:59.999
2. Insert records at 00:00:00.000
3. Query across midnight boundary
4. Verify correct table/partition assignment

### Test 8.2: Maximum Limits Testing
**Objective:** Verify system limits

**Test Cases:**
- Maximum batch size: 10,000 records
- Maximum table name length
- Maximum concurrent connections: 100
- Maximum partitions per table: 1024 (MySQL limit)
- Maximum ID value (BIGINT max)

### Test 8.3: Special Characters and Encoding
**Objective:** Verify UTF-8 support

**Test Data:**
- Emoji: 😀🎉
- Chinese: 你好世界
- Arabic: مرحبا بالعالم
- Special: <script>alert('xss')</script>

---

## Test Execution Checklist

### Pre-Test Validation
- [ ] MySQL is running and accessible
- [ ] Test database is clean
- [ ] Sufficient disk space (10GB+)
- [ ] Monitoring tools ready

### Test Execution Order
1. [ ] Multi-table basic tests (1.1-1.3)
2. [ ] Partitioned table basic tests (2.1-2.2)
3. [ ] Data integrity tests (3.1-3.2)
4. [ ] Performance baseline (4.1-4.2)
5. [ ] Failure scenarios (5.1-5.2)
6. [ ] Migration tests (6.1-6.2)
7. [ ] Edge cases (8.1-8.3)
8. [ ] 24-hour stability test (4.3)

### Post-Test Cleanup
- [ ] Drop test databases
- [ ] Archive test logs
- [ ] Document any issues found
- [ ] Performance metrics summary

---

## Success Metrics

### Critical (Must Pass)
- Zero data loss
- Zero data corruption  
- Automatic recovery from failures
- Correct partition/table routing

### Important (Should Pass)
- Insert TPS > 1000 (batch mode)
- Query response < 100ms (P95)
- Memory stable over 24 hours
- Monitoring metrics accurate

### Nice to Have
- Insert TPS > 5000 (optimized)
- Query response < 10ms (P95)
- Zero errors in 24-hour test

---

## Risk Areas Requiring Special Attention

1. **Concurrent Operations**: Race conditions in partition creation
2. **Boundary Conditions**: Day/hour transitions
3. **Resource Exhaustion**: Connection pool, memory, disk
4. **Maintenance Windows**: Data availability during maintenance
5. **Clock Skew**: Systems with incorrect time
6. **Network Partitions**: Split-brain scenarios
7. **ID Collisions**: Auto-increment across tables/partitions

---

## Tools and Scripts Required

### MySQL Monitoring Queries
```sql
-- Connection monitoring
SHOW STATUS LIKE 'Threads_connected';
SHOW PROCESSLIST;

-- Performance monitoring  
SHOW STATUS LIKE 'Handler%';
SHOW STATUS LIKE 'Innodb_buffer_pool%';

-- Table statistics
SELECT 
  table_name,
  table_rows,
  data_length/1024/1024 as data_mb,
  index_length/1024/1024 as index_mb
FROM information_schema.tables
WHERE table_schema = 'test';

-- Partition statistics
SELECT 
  table_name,
  partition_name,
  table_rows,
  data_length/1024/1024 as data_mb
FROM information_schema.partitions
WHERE table_schema = 'test'
ORDER BY table_name, partition_ordinal_position;
```

### System Monitoring Commands
```bash
# Disk I/O
iostat -x 1

# Memory usage
free -h

# Process monitoring
top -p <java_pid>

# Network connections
netstat -an | grep 3306 | wc -l
```

---

## Test Report Template

```
Test Run ID: ___________
Date: ___________
Environment: ___________
MySQL Version: ___________

Summary:
- Tests Passed: ___/___
- Critical Issues: ___
- Performance TPS: ___
- P95 Query Time: ___ms

Issues Found:
1. _____________
2. _____________

Recommendations:
1. _____________
2. _____________

Approved By: ___________
```