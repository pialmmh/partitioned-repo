# Multi-Table Repository Detailed Test Procedures

## Manual Testing Walkthrough

### Scenario 1: Real-Time SMS Traffic Simulation

**Business Context:** Simulating a telecom SMS gateway processing 1M messages/day

#### Test Setup
```sql
-- Prepare database
CREATE DATABASE IF NOT EXISTS sms_test;
USE sms_test;

-- Create monitoring table for verification
CREATE TABLE test_metrics (
  test_id VARCHAR(50),
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  records_inserted INT,
  tables_created INT,
  errors INT,
  notes TEXT
);
```

#### Hour-by-Hour Test Execution

**Hour 0-1: Initial Load**
1. Start repository with `sms` prefix
2. Insert 42,000 records (1M/24h rate):
   - Distribution: Every 2 seconds, insert 23-24 records
   - Timestamps: Use current time for realistic distribution
3. Monitor table creation:
   ```sql
   -- Run every 5 minutes
   SELECT COUNT(*) as table_count,
          SUM(table_rows) as total_rows,
          MIN(table_name) as first_table,
          MAX(table_name) as last_table
   FROM information_schema.tables
   WHERE table_schema='sms_test' AND table_name LIKE 'sms_%';
   ```

**Hour 2-6: Sustained Load**
1. Continue inserting at 42K records/hour
2. Every 30 minutes, perform:
   - Random ID lookup (simulate customer inquiry)
   - Date range query for last hour
   - Batch fetch of 100 recent messages
3. Track query response times manually

**Hour 7: Peak Load Simulation**
1. Increase to 100K records in this hour
2. Simulate multiple clients:
   - Open 5 terminal sessions
   - Each inserts 20K records concurrently
3. Monitor for:
   - Table lock waits
   - Connection pool exhaustion
   - Error rates

**Hour 8-23: Mixed Workload**
1. Return to 42K inserts/hour
2. Add read operations:
   - Every minute: Find messages from specific hour
   - Every 5 minutes: Cursor iteration through 1000 records
   - Every 10 minutes: Batch processing simulation

**Hour 24: Maintenance Trigger**
1. Check if maintenance runs automatically
2. Verify future tables created
3. No old tables dropped (all within retention)

#### Verification Queries

```sql
-- Hourly partition distribution within daily table
SELECT 
  PARTITION_NAME,
  TABLE_ROWS,
  AVG_ROW_LENGTH,
  DATA_LENGTH/1024 as data_kb
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA='sms_test' 
  AND TABLE_NAME='sms_20250104'
  AND PARTITION_NAME IS NOT NULL
ORDER BY PARTITION_ORDINAL_POSITION;

-- Check for hot partitions
SELECT 
  HOUR(created_at) as hour,
  COUNT(*) as msg_count,
  MIN(id) as min_id,
  MAX(id) as max_id
FROM sms_20250104
GROUP BY HOUR(created_at)
ORDER BY hour;

-- Verify no orphaned records
SELECT COUNT(*) 
FROM sms_20250104
WHERE DATE(created_at) != '2025-01-04';
```

---

### Scenario 2: Historical Data Migration

**Business Context:** Migrating 90 days of historical SMS data

#### Preparation Phase
1. Generate historical data CSV:
   - 90 files, one per day
   - Each file: 500K records
   - Total: 45M records

2. Create tracking table:
```sql
CREATE TABLE migration_progress (
  day_date DATE PRIMARY KEY,
  records_expected INT,
  records_migrated INT,
  migration_start TIMESTAMP,
  migration_end TIMESTAMP,
  status ENUM('pending', 'in_progress', 'completed', 'failed'),
  error_message TEXT
);
```

#### Migration Execution

**Day-by-Day Import Process:**
```
For each historical day (starting from oldest):
1. Mark day as 'in_progress' in tracking table
2. Read CSV file for that day
3. Insert in batches of 5000
4. Verify count matches
5. Mark as 'completed' or 'failed'
```

#### Validation Steps

**After Each Day Import:**
```sql
-- Verify table was created
SELECT table_name, table_rows
FROM information_schema.tables  
WHERE table_schema='sms_test' 
  AND table_name = CONCAT('sms_', DATE_FORMAT(@import_date, '%Y%m%d'));

-- Verify hourly distribution
SELECT 
  HOUR(created_at) as hour,
  COUNT(*) as records
FROM sms_YYYYMMDD
GROUP BY HOUR(created_at);

-- Check for data integrity
SELECT 
  MD5(GROUP_CONCAT(id ORDER BY id)) as checksum
FROM sms_YYYYMMDD;
-- Compare with source CSV checksum
```

**After Full Migration:**
```sql
-- Verify all 90 tables exist
SELECT COUNT(DISTINCT table_name) as table_count
FROM information_schema.tables
WHERE table_schema='sms_test' AND table_name LIKE 'sms_%';

-- Verify total record count
SELECT SUM(table_rows) as total_records
FROM information_schema.tables  
WHERE table_schema='sms_test' AND table_name LIKE 'sms_%';

-- Find any gaps in daily tables
SELECT 
  DATE_ADD('2024-10-06', INTERVAL n DAY) as expected_date
FROM (
  SELECT a.N + b.N * 10 as n
  FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
  CROSS JOIN (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) b
) numbers
WHERE n < 90
  AND CONCAT('sms_', DATE_FORMAT(DATE_ADD('2024-10-06', INTERVAL n DAY), '%Y%m%d'))
  NOT IN (
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema='sms_test'
  );
```

---

### Scenario 3: Maintenance Window Behavior

**Business Context:** Verify data availability during maintenance operations

#### Test Setup
1. Configure retention = 7 days
2. Pre-populate 14 days of data:
   - Days -13 to -7: Old data (will be deleted)
   - Days -6 to 0: Current data (will be kept)
   - Days +1 to +0: No data yet

#### Live Maintenance Test

**Pre-Maintenance State:**
```sql
-- Record current state
INSERT INTO test_metrics 
SELECT 
  'pre_maintenance',
  NOW(),
  NULL,
  SUM(table_rows),
  COUNT(*),
  0,
  GROUP_CONCAT(table_name)
FROM information_schema.tables
WHERE table_schema='sms_test' AND table_name LIKE 'sms_%';
```

**During Maintenance:**
1. Start continuous read/write operations:
   ```
   Terminal 1: Insert 10 records/second
   Terminal 2: Query random records every second
   Terminal 3: Run date range queries continuously
   ```

2. Trigger maintenance (or wait for scheduled)

3. Monitor for:
   - Query failures
   - Insert failures  
   - Lock waits
   - Response time spikes

**Post-Maintenance Verification:**
```sql
-- Verify old tables dropped
SELECT COUNT(*) as old_tables_remaining
FROM information_schema.tables
WHERE table_schema='sms_test' 
  AND table_name < CONCAT('sms_', DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 7 DAY), '%Y%m%d'));

-- Verify future tables created
SELECT COUNT(*) as future_tables
FROM information_schema.tables  
WHERE table_schema='sms_test'
  AND table_name > CONCAT('sms_', DATE_FORMAT(NOW(), INTERVAL 0 DAY), '%Y%m%d'));

-- Check for data loss in retained period
SELECT 
  DATE(created_at) as data_date,
  COUNT(*) as record_count
FROM (
  SELECT created_at FROM sms_20250104
  UNION ALL
  SELECT created_at FROM sms_20250103
  -- ... for all retained tables
) combined
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE(created_at);
```

---

### Scenario 4: Concurrent Access Patterns

**Business Context:** Multiple applications accessing same data

#### Test Applications Setup

**App 1: Real-time Inserter**
- Inserts 100 records every 5 seconds
- Uses batch insert
- Records: Current timestamp

**App 2: Analytics Reader**
- Every minute: Count last hour's messages
- Every 5 minutes: Generate hourly report
- Every hour: Full day summary

**App 3: Customer Service Tool**
- Random ID lookups (10/minute)
- Find messages by ID range
- Pagination through results

**App 4: Maintenance Process**
- Triggers every hour
- Creates tomorrow's table
- Drops old tables

#### Concurrency Verification

```sql
-- Monitor lock contention
SELECT 
  thread_id,
  object_schema,
  object_name,
  lock_type,
  lock_status,
  lock_data
FROM performance_schema.data_locks
WHERE object_schema = 'sms_test'
ORDER BY thread_id;

-- Check for blocking queries
SELECT 
  blocking_pid,
  blocking_query,
  blocked_pid,
  blocked_query,
  wait_age
FROM sys.innodb_lock_waits;

-- Monitor connection pool usage
SHOW STATUS LIKE 'Threads_connected';
SHOW STATUS LIKE 'Max_used_connections';
```

#### Expected Behavior Checklist

**During 1-Hour Concurrent Test:**
- [ ] No deadlocks occur
- [ ] All inserts complete successfully
- [ ] Read queries return consistent data
- [ ] No query takes > 1 second
- [ ] Connection count stays < 50
- [ ] No table metadata locks > 100ms
- [ ] Maintenance completes < 5 seconds
- [ ] Zero data loss or corruption

---

### Scenario 5: Recovery Testing

**Business Context:** System recovery after various failure modes

#### Test Case 1: Mid-Batch Failure

1. Start inserting batch of 10,000 records
2. At ~5,000 records, kill -9 the Java process
3. Check database state:
```sql
-- Find partial batch
SELECT 
  COUNT(*) as partial_count,
  MIN(id) as min_id,
  MAX(id) as max_id
FROM sms_20250104
WHERE id BETWEEN @batch_start AND @batch_end;
```
4. Restart application
5. Retry same batch insert
6. Verify no duplicates

#### Test Case 2: Table Creation Race Condition

1. Start two repository instances simultaneously
2. Both try to insert data for new day
3. Monitor for:
   - "Table already exists" errors
   - Duplicate key errors
   - Both succeed without errors
4. Verify table structure is correct

#### Test Case 3: Partition Creation Failure

1. Fill disk to 99% capacity
2. Attempt to insert data for new hour
3. Verify error handling:
   - Clear error message
   - No partial partition
   - No data corruption
4. Free disk space
5. Retry operation
6. Verify success

---

## Performance Baseline Measurements

### Expected Performance Metrics

| Operation | Record Count | Expected Time | Acceptable Range |
|-----------|-------------|---------------|------------------|
| Single Insert | 1 | < 5ms | 1-10ms |
| Batch Insert | 1000 | < 100ms | 50-200ms |
| Batch Insert | 10000 | < 1s | 0.5-2s |
| Find by ID (single table) | 1 | < 2ms | 1-5ms |
| Find by ID (multi-table scan) | 1 | < 50ms | 10-100ms |
| Date range (1 hour) | ~1700 | < 20ms | 10-50ms |
| Date range (1 day) | ~42000 | < 200ms | 100-500ms |
| Date range (7 days) | ~300000 | < 2s | 1-5s |
| Cursor iteration | 100/batch | < 10ms | 5-20ms |
| Table creation | 1 | < 100ms | 50-500ms |
| Partition creation | 1 | < 50ms | 20-200ms |
| Maintenance cycle | varies | < 5s | 2-10s |

### Manual Performance Testing Steps

1. **Prepare Test Data Generator**
   - Generate records with realistic data
   - Include variety in message lengths
   - Use current timestamps

2. **Measure Each Operation**
   ```sql
   -- Before operation
   SELECT @start := NOW(6);
   
   -- After operation  
   SELECT TIMESTAMPDIFF(MICROSECOND, @start, NOW(6))/1000 as ms_elapsed;
   ```

3. **Record in Spreadsheet**
   - Operation type
   - Record count
   - Time taken
   - Table count at time
   - Total records in system
   - Notes on anomalies

4. **Identify Bottlenecks**
   - Operations exceeding acceptable range
   - Degradation with data growth
   - Resource constraints (CPU, I/O, Memory)

---

## Manual Test Execution Log Template

```
Date: _________
Tester: _________
Environment: _________

Test Scenario: _________
Start Time: _________
End Time: _________

Steps Executed:
1. _________ [✓/✗] Notes: _________
2. _________ [✓/✗] Notes: _________
3. _________ [✓/✗] Notes: _________

Issues Found:
- _________
- _________

Performance Observations:
- Insert rate: _____ records/sec
- Query response: _____ ms average
- Resource usage: CPU ___%, Memory ___MB

Database State:
- Tables created: _____
- Total records: _____
- Disk usage: _____ GB

Recommendations:
_________
_________

Next Steps:
_________
_________
```