# Partitioned Table Repository Detailed Test Procedures

## Manual Testing Walkthrough

### Scenario 1: E-Commerce Order Processing Simulation

**Business Context:** E-commerce platform processing 100K orders/day with MySQL native partitioning

#### Initial Setup and Verification

```sql
-- Prepare test environment
CREATE DATABASE IF NOT EXISTS orders_test;
USE orders_test;

-- Create verification procedure
DELIMITER $$
CREATE PROCEDURE check_partition_status()
BEGIN
  SELECT 
    PARTITION_NAME,
    PARTITION_EXPRESSION,
    PARTITION_DESCRIPTION,
    TABLE_ROWS,
    DATA_LENGTH/1024/1024 as data_mb,
    INDEX_LENGTH/1024/1024 as index_mb,
    CREATE_TIME
  FROM information_schema.PARTITIONS
  WHERE TABLE_SCHEMA = 'orders_test' 
    AND TABLE_NAME = 'orders'
    AND PARTITION_NAME IS NOT NULL
  ORDER BY PARTITION_ORDINAL_POSITION;
END$$
DELIMITER ;
```

#### Day 1: Initial Table Creation Test

**Morning (9 AM):**
1. Start repository with table name `orders`
2. Insert first order
3. Verify table structure:
```sql
SHOW CREATE TABLE orders\G

-- Expected output should show:
-- PARTITION BY RANGE (TO_DAYS(created_at))
-- At least one partition for today
```

**Hourly Operations (10 AM - 6 PM):**
- Every hour: Insert 4,000 orders (32K total)
- Track partition growth:
```sql
CALL check_partition_status();

-- Monitor AUTO_INCREMENT progress
SELECT AUTO_INCREMENT 
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'orders_test' AND TABLE_NAME = 'orders';
```

**End of Day Verification:**
```sql
-- Verify all orders are in correct partition
SELECT 
  DATE(created_at) as order_date,
  COUNT(*) as order_count,
  MIN(id) as first_order_id,
  MAX(id) as last_order_id
FROM orders
GROUP BY DATE(created_at);

-- Check partition pruning effectiveness
EXPLAIN PARTITIONS 
SELECT * FROM orders 
WHERE created_at >= CURDATE() AND created_at < CURDATE() + INTERVAL 1 DAY;
```

#### Day 2-7: Multi-Day Operation Test

**Daily Pattern:**
1. Morning: Insert 10K orders between 6-9 AM
2. Peak: Insert 50K orders between 10 AM-2 PM  
3. Afternoon: Insert 30K orders between 2-6 PM
4. Evening: Insert 10K orders between 6-10 PM

**Daily Verification Checklist:**
```sql
-- New partition created automatically?
SELECT COUNT(DISTINCT PARTITION_NAME) as partition_count
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'orders_test' AND TABLE_NAME = 'orders';

-- Data in correct partitions?
SELECT 
  p.PARTITION_NAME,
  p.PARTITION_DESCRIPTION,
  COUNT(o.id) as actual_rows,
  p.TABLE_ROWS as reported_rows
FROM information_schema.PARTITIONS p
LEFT JOIN orders o ON DATE(o.created_at) = DATE(FROM_DAYS(
  CAST(REPLACE(REPLACE(p.PARTITION_DESCRIPTION, 'TO_DAYS(''', ''), ''')', '') AS DATE)
))
WHERE p.TABLE_SCHEMA = 'orders_test' 
  AND p.TABLE_NAME = 'orders'
  AND p.PARTITION_NAME IS NOT NULL
GROUP BY p.PARTITION_NAME;
```

#### Day 8: Historical Data Test

**Objective:** Verify partition creation for past and future dates

**Test Steps:**
1. Insert orders with timestamps from 30 days ago:
```sql
-- This should trigger partition creation for past date
INSERT INTO orders (customer_id, amount, status, created_at) 
VALUES (1001, 99.99, 'completed', DATE_SUB(NOW(), INTERVAL 30 DAY));
```

2. Verify partition was created:
```sql
CALL check_partition_status();
-- Should see new partition for 30 days ago
```

3. Insert order for 30 days in future:
```sql
INSERT INTO orders (customer_id, amount, status, created_at)
VALUES (1002, 199.99, 'pending', DATE_ADD(NOW(), INTERVAL 30 DAY));
```

4. Verify future partition created

5. Check partition ordering:
```sql
-- Partitions should be in chronological order
SELECT 
  PARTITION_NAME,
  PARTITION_DESCRIPTION,
  PARTITION_ORDINAL_POSITION
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'orders_test' AND TABLE_NAME = 'orders'
ORDER BY PARTITION_ORDINAL_POSITION;
```

---

### Scenario 2: Partition Pruning Performance Test

**Business Context:** Verify MySQL properly prunes partitions for better query performance

#### Setup: Create 365 Days of Data

```sql
-- Generate procedure to create test data
DELIMITER $$
CREATE PROCEDURE generate_year_data()
BEGIN
  DECLARE v_date DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 364 DAY);
  DECLARE v_counter INT DEFAULT 0;
  
  WHILE v_date <= CURDATE() DO
    -- Insert 1000 orders for each day
    INSERT INTO orders (customer_id, amount, status, created_at)
    SELECT 
      FLOOR(RAND() * 10000) + 1,
      ROUND(RAND() * 1000, 2),
      ELT(FLOOR(RAND() * 3) + 1, 'pending', 'processing', 'completed'),
      v_date + INTERVAL FLOOR(RAND() * 86400) SECOND
    FROM 
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t6,
      (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t7;
    
    SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    SET v_counter = v_counter + 1;
    
    IF v_counter % 30 = 0 THEN
      SELECT CONCAT('Generated data up to: ', v_date) as progress;
    END IF;
  END WHILE;
END$$
DELIMITER ;

-- Execute data generation
CALL generate_year_data();
```

#### Partition Pruning Tests

**Test 1: Single Day Query**
```sql
-- Enable profiling
SET profiling = 1;

-- Query single day (should scan 1 partition)
SELECT COUNT(*), SUM(amount) 
FROM orders 
WHERE created_at >= '2025-01-04' AND created_at < '2025-01-05';

-- Check execution plan
EXPLAIN PARTITIONS 
SELECT COUNT(*), SUM(amount) 
FROM orders 
WHERE created_at >= '2025-01-04' AND created_at < '2025-01-05';

-- Check profile
SHOW PROFILE;

-- Expected: Only partition p20250104 accessed
```

**Test 2: Week Range Query**
```sql
-- Query one week (should scan 7 partitions)
SELECT COUNT(*), SUM(amount)
FROM orders
WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) 
  AND created_at < CURDATE();

EXPLAIN PARTITIONS 
SELECT COUNT(*), SUM(amount)
FROM orders
WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) 
  AND created_at < CURDATE();

-- Verify only 7 partitions scanned, not all 365
```

**Test 3: Month Range Query**
```sql
-- Query one month (should scan ~30 partitions)
SELECT 
  DATE(created_at) as order_date,
  COUNT(*) as daily_orders,
  SUM(amount) as daily_revenue
FROM orders
WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(created_at);

EXPLAIN PARTITIONS 
SELECT DATE(created_at) as order_date, COUNT(*), SUM(amount)
FROM orders
WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(created_at);
```

**Test 4: Query Without Date Filter (Full Scan)**
```sql
-- This should scan ALL partitions (worst case)
EXPLAIN PARTITIONS
SELECT * FROM orders WHERE amount > 500;

-- Compare with date-filtered version
EXPLAIN PARTITIONS  
SELECT * FROM orders 
WHERE amount > 500 
  AND created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY);
```

#### Performance Comparison Matrix

| Query Type | Partitions Scanned | Expected Time | Without Partitioning |
|------------|-------------------|---------------|---------------------|
| Single Day | 1 | < 10ms | ~300ms |
| Week Range | 7 | < 50ms | ~300ms |
| Month Range | 30 | < 200ms | ~300ms |
| Quarter Range | 90 | < 600ms | ~300ms |
| Full Year | 365 | ~2s | ~300ms |
| No Date Filter | 365 | ~2s | ~2s |

---

### Scenario 3: Partition Maintenance Operations

**Business Context:** Managing partition lifecycle - creation, optimization, dropping

#### Test 1: Maximum Partitions Limit

```sql
-- Check current partition count
SELECT COUNT(*) as current_partitions
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'orders_test' 
  AND TABLE_NAME = 'orders'
  AND PARTITION_NAME IS NOT NULL;

-- MySQL limit is 1024 partitions per table
-- Test approaching this limit
```

**Creating Many Partitions:**
```sql
-- Try to create 1000 days of partitions
DELIMITER $$
CREATE PROCEDURE create_future_partitions()
BEGIN
  DECLARE v_date DATE DEFAULT CURDATE();
  DECLARE v_counter INT DEFAULT 0;
  
  WHILE v_counter < 1000 DO
    SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    
    -- Insert dummy record to trigger partition creation
    INSERT INTO orders (customer_id, amount, status, created_at)
    VALUES (1, 0.01, 'test', v_date);
    
    -- Delete the dummy record
    DELETE FROM orders 
    WHERE created_at >= v_date AND created_at < DATE_ADD(v_date, INTERVAL 1 DAY)
      AND amount = 0.01;
    
    SET v_counter = v_counter + 1;
    
    IF v_counter % 100 = 0 THEN
      SELECT v_counter as partitions_created;
    END IF;
  END WHILE;
END$$
DELIMITER ;

-- Monitor partition creation
CALL create_future_partitions();
```

**Verify System Behavior Near Limit:**
```sql
-- Check if we're near limit
SELECT 
  COUNT(*) as partition_count,
  1024 - COUNT(*) as remaining_capacity
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'orders_test' 
  AND TABLE_NAME = 'orders'
  AND PARTITION_NAME IS NOT NULL;

-- Test what happens when limit is exceeded
-- Expected: Error message about partition limit
```

#### Test 2: Partition Dropping

```sql
-- Manual partition drop (for old data)
ALTER TABLE orders DROP PARTITION p20240101;

-- Verify data in that partition is gone
SELECT COUNT(*) 
FROM orders 
WHERE created_at >= '2024-01-01' AND created_at < '2024-01-02';
-- Should return 0

-- Verify partition list updated
CALL check_partition_status();
```

#### Test 3: Partition Reorganization

```sql
-- Merge multiple partitions into one
ALTER TABLE orders 
REORGANIZE PARTITION p20250101,p20250102,p20250103 
INTO (
  PARTITION p202501_first3days VALUES LESS THAN (TO_DAYS('2025-01-04'))
);

-- Verify data is preserved
SELECT 
  DATE(created_at) as date,
  COUNT(*) as orders
FROM orders
WHERE created_at >= '2025-01-01' AND created_at < '2025-01-04'
GROUP BY DATE(created_at);
```

---

### Scenario 4: Concurrent Operations on Partitioned Table

**Business Context:** Multiple processes accessing different partitions simultaneously

#### Test Setup: 4 Concurrent Processes

**Process 1: Current Day Inserter**
```sql
-- Continuous inserts to today's partition
DELIMITER $$
CREATE PROCEDURE insert_today_orders()
BEGIN
  DECLARE v_counter INT DEFAULT 0;
  WHILE v_counter < 10000 DO
    INSERT INTO orders (customer_id, amount, status, created_at)
    VALUES (
      FLOOR(RAND() * 10000),
      ROUND(RAND() * 1000, 2),
      'new',
      NOW()
    );
    SET v_counter = v_counter + 1;
    IF v_counter % 1000 = 0 THEN
      SELECT CONCAT('Process 1: Inserted ', v_counter) as status;
      DO SLEEP(1);
    END IF;
  END WHILE;
END$$
DELIMITER ;
```

**Process 2: Historical Data Updater**
```sql
-- Updates orders from last 30 days
DELIMITER $$
CREATE PROCEDURE update_historical_orders()
BEGIN
  DECLARE v_counter INT DEFAULT 0;
  WHILE v_counter < 1000 DO
    UPDATE orders 
    SET status = 'processed',
        updated_at = NOW()
    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
      AND created_at < DATE_SUB(CURDATE(), INTERVAL 29 DAY)
      AND status = 'pending'
    LIMIT 10;
    
    SET v_counter = v_counter + 1;
    IF v_counter % 100 = 0 THEN
      SELECT CONCAT('Process 2: Updated batch ', v_counter) as status;
    END IF;
  END WHILE;
END$$
DELIMITER ;
```

**Process 3: Analytics Reader**
```sql
-- Reads aggregated data from different partitions
DELIMITER $$
CREATE PROCEDURE read_analytics()
BEGIN
  DECLARE v_counter INT DEFAULT 0;
  WHILE v_counter < 100 DO
    -- Daily summary for random day in last 30 days
    SELECT 
      DATE(created_at) as date,
      COUNT(*) as orders,
      SUM(amount) as revenue,
      AVG(amount) as avg_order_value
    FROM orders
    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 30) DAY)
      AND created_at < DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 30) - 1 DAY)
    GROUP BY DATE(created_at);
    
    SET v_counter = v_counter + 1;
    DO SLEEP(2);
  END WHILE;
END$$
DELIMITER ;
```

**Process 4: Maintenance Operations**
```sql
-- Creates future partitions and drops old ones
DELIMITER $$
CREATE PROCEDURE maintenance_operations()
BEGIN
  -- Create partition for tomorrow if not exists
  INSERT INTO orders (customer_id, amount, status, created_at)
  VALUES (1, 0.01, 'maintenance', DATE_ADD(CURDATE(), INTERVAL 1 DAY));
  
  DELETE FROM orders 
  WHERE amount = 0.01 AND status = 'maintenance';
  
  -- Drop partitions older than 365 days
  -- (Would need dynamic SQL in real scenario)
  
  SELECT 'Maintenance completed' as status;
END$$
DELIMITER ;
```

#### Concurrent Execution Test

1. Open 4 MySQL sessions
2. Start all procedures simultaneously:
   - Session 1: `CALL insert_today_orders();`
   - Session 2: `CALL update_historical_orders();`
   - Session 3: `CALL read_analytics();`
   - Session 4: `CALL maintenance_operations();`

3. Monitor for issues:
```sql
-- Check for lock waits
SELECT * FROM sys.innodb_lock_waits;

-- Monitor partition lock status
SELECT 
  object_name,
  lock_type,
  lock_mode,
  lock_status,
  thread_id
FROM performance_schema.metadata_locks
WHERE object_schema = 'orders_test';

-- Check for long-running queries
SELECT 
  id,
  user,
  time,
  state,
  info
FROM information_schema.processlist
WHERE db = 'orders_test' AND time > 5;
```

#### Expected Results
- [ ] All processes complete without deadlocks
- [ ] No partition-level blocking between processes
- [ ] Each process works on different partitions without interference
- [ ] Performance remains consistent

---

### Scenario 5: Data Integrity Validation

**Business Context:** Ensure no data loss during partition operations

#### Test 1: ID Uniqueness Across Partitions

```sql
-- Check for duplicate IDs across all partitions
SELECT id, COUNT(*) as occurrences
FROM orders
GROUP BY id
HAVING COUNT(*) > 1;
-- Expected: Empty result set

-- Verify AUTO_INCREMENT consistency
SELECT 
  MIN(id) as min_id,
  MAX(id) as max_id,
  COUNT(*) as total_records,
  MAX(id) - MIN(id) + 1 as expected_records,
  COUNT(*) - (MAX(id) - MIN(id) + 1) as difference
FROM orders;
-- Difference should be 0 or negative (due to deletes)
```

#### Test 2: Partition Boundary Data Integrity

```sql
-- Check records at partition boundaries (23:59:59)
SELECT 
  id,
  created_at,
  DATE(created_at) as date_part,
  TIME(created_at) as time_part
FROM orders
WHERE TIME(created_at) >= '23:59:00'
ORDER BY created_at;

-- Verify each record is in correct partition
SELECT 
  p.PARTITION_NAME,
  DATE(o.created_at) as order_date,
  COUNT(*) as records
FROM orders o
JOIN information_schema.PARTITIONS p
  ON p.TABLE_SCHEMA = 'orders_test' 
  AND p.TABLE_NAME = 'orders'
  AND DATE(o.created_at) = DATE(
    SUBDATE(FROM_DAYS(
      CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(p.PARTITION_DESCRIPTION, '(', -1), ')', 1) AS UNSIGNED)
    ), 1)
  )
WHERE TIME(o.created_at) >= '23:59:00'
GROUP BY p.PARTITION_NAME, DATE(o.created_at);
```

#### Test 3: Referential Integrity

```sql
-- If there are foreign keys, verify they work across partitions
-- Create related table
CREATE TABLE order_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id INT,
  quantity INT,
  price DECIMAL(10,2),
  KEY idx_order_id (order_id)
);

-- Insert items for orders across different partitions
INSERT INTO order_items (order_id, product_id, quantity, price)
SELECT 
  id,
  FLOOR(RAND() * 1000),
  FLOOR(RAND() * 5) + 1,
  ROUND(RAND() * 100, 2)
FROM orders
WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
LIMIT 1000;

-- Verify joins work correctly across partitions
SELECT 
  DATE(o.created_at) as order_date,
  COUNT(DISTINCT o.id) as orders,
  COUNT(oi.id) as order_items,
  SUM(oi.quantity * oi.price) as total_value
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY DATE(o.created_at);
```

---

## Manual Test Checklist

### Pre-Test Setup
- [ ] MySQL version verified (5.7+ or 8.0+)
- [ ] Partition support confirmed: `SHOW PLUGINS WHERE Name = 'partition'`
- [ ] Test database created
- [ ] Monitoring queries prepared
- [ ] Test data generators ready

### During Test Execution
- [ ] Take screenshots of partition structure
- [ ] Record query execution times
- [ ] Monitor system resources (CPU, Memory, Disk I/O)
- [ ] Check MySQL error log for warnings
- [ ] Document any unexpected behavior

### Post-Test Validation
- [ ] All partitions have correct data
- [ ] No orphaned records
- [ ] Query performance meets expectations
- [ ] No data loss or corruption
- [ ] AUTO_INCREMENT values consistent

### Performance Benchmarks to Record

| Metric | Value | Target | Pass/Fail |
|--------|-------|--------|-----------|
| Partition creation time | ___ ms | < 100ms | [ ] |
| Single partition query | ___ ms | < 10ms | [ ] |
| Multi-partition query (7 days) | ___ ms | < 50ms | [ ] |
| Full table scan | ___ sec | < 5s | [ ] |
| Concurrent insert rate | ___ /sec | > 1000 | [ ] |
| Partition drop time | ___ ms | < 500ms | [ ] |
| Memory usage per partition | ___ MB | < 10MB | [ ] |
| Max partitions supported | ___ | 1024 | [ ] |

### Common Issues and Troubleshooting

1. **"Table has no partition for value"**
   - Cause: Trying to insert future/past date without partition
   - Fix: Ensure partition creation logic handles edge cases

2. **"Too many partitions"**
   - Cause: Exceeded 1024 partition limit
   - Fix: Implement partition rotation/archival

3. **Slow queries despite partitioning**
   - Cause: Missing date filter in WHERE clause
   - Fix: Always include partitioning key in queries

4. **AUTO_INCREMENT gaps**
   - Cause: Normal behavior with partitions
   - Note: Each partition may reserve ID ranges