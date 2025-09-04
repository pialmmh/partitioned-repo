# Performance and Stress Test Procedures

## Performance Testing Framework

### Test Environment Requirements

**Hardware Specifications:**
- CPU: Minimum 4 cores, recommended 8 cores
- RAM: Minimum 8GB, recommended 16GB
- Disk: SSD with minimum 100GB free space
- Network: Gigabit connection for distributed testing

**MySQL Configuration for Performance Testing:**
```sql
-- Check current settings
SHOW VARIABLES LIKE 'max_connections';
SHOW VARIABLES LIKE 'innodb_buffer_pool_size';
SHOW VARIABLES LIKE 'innodb_log_file_size';
SHOW VARIABLES LIKE 'innodb_flush_log_at_trx_commit';

-- Recommended performance settings
SET GLOBAL max_connections = 500;
SET GLOBAL innodb_buffer_pool_size = 4294967296; -- 4GB
SET GLOBAL innodb_flush_log_at_trx_commit = 2; -- Better performance, slight risk
SET GLOBAL innodb_flush_method = O_DIRECT;
SET GLOBAL innodb_file_per_table = ON;
```

### Performance Monitoring Setup

```sql
-- Create performance tracking tables
CREATE DATABASE IF NOT EXISTS perf_test;
USE perf_test;

CREATE TABLE test_results (
  test_id VARCHAR(50) PRIMARY KEY,
  test_name VARCHAR(100),
  start_time TIMESTAMP(6),
  end_time TIMESTAMP(6),
  duration_ms BIGINT,
  operations INT,
  operations_per_sec DECIMAL(10,2),
  avg_latency_ms DECIMAL(10,3),
  p95_latency_ms DECIMAL(10,3),
  p99_latency_ms DECIMAL(10,3),
  errors INT,
  notes TEXT
);

CREATE TABLE test_metrics (
  metric_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  test_id VARCHAR(50),
  timestamp TIMESTAMP(6),
  metric_name VARCHAR(50),
  metric_value DECIMAL(20,3),
  KEY idx_test_timestamp (test_id, timestamp)
);
```

---

## Test Suite 1: Insert Performance Tests

### Test 1.1: Single Thread Insert Performance

**Objective:** Establish baseline insert performance

**Test Procedure:**

1. **Warm-up Phase:**
```sql
-- Clear caches
RESET QUERY CACHE;
FLUSH TABLES;

-- Warm up connection pool
-- Execute 100 dummy inserts
```

2. **Test Execution:**
```
Test Parameters:
- Record counts: [100, 1000, 10000, 100000]
- Batch sizes: [1, 10, 100, 1000]
- Test duration: Until all records inserted
```

3. **Measurement Points:**
```sql
-- Before test
SELECT @test_start := NOW(6);
SELECT @initial_rows := COUNT(*) FROM target_table;

-- After test
SELECT @test_end := NOW(6);
SELECT @final_rows := COUNT(*) FROM target_table;

-- Calculate metrics
SELECT 
  TIMESTAMPDIFF(MICROSECOND, @test_start, @test_end)/1000 as duration_ms,
  @final_rows - @initial_rows as records_inserted,
  (@final_rows - @initial_rows) / (TIMESTAMPDIFF(MICROSECOND, @test_start, @test_end)/1000000) as inserts_per_sec;
```

**Expected Results Matrix:**

| Batch Size | Records | Expected Time | Target TPS | Actual TPS |
|------------|---------|--------------|------------|------------|
| 1 | 1000 | < 2s | > 500 | ___ |
| 10 | 1000 | < 0.5s | > 2000 | ___ |
| 100 | 10000 | < 2s | > 5000 | ___ |
| 1000 | 100000 | < 10s | > 10000 | ___ |

### Test 1.2: Multi-Thread Insert Performance

**Objective:** Test concurrent insert scalability

**Test Configuration:**
```
Thread counts: [2, 4, 8, 16, 32]
Records per thread: 10000
Batch size: 100
```

**Execution Script Pattern:**
```sql
-- Thread simulation using multiple connections
-- Each connection executes:
DELIMITER $$
CREATE PROCEDURE concurrent_insert_test(IN thread_id INT)
BEGIN
  DECLARE v_counter INT DEFAULT 0;
  DECLARE v_batch_start TIMESTAMP(6);
  DECLARE v_batch_end TIMESTAMP(6);
  
  WHILE v_counter < 10000 DO
    SET v_batch_start = NOW(6);
    
    -- Insert batch of 100
    INSERT INTO target_table (...)
    VALUES (...), (...), ... ; -- 100 values
    
    SET v_batch_end = NOW(6);
    
    -- Log performance
    INSERT INTO test_metrics (test_id, timestamp, metric_name, metric_value)
    VALUES (
      CONCAT('concurrent_', thread_id),
      v_batch_end,
      'batch_latency_ms',
      TIMESTAMPDIFF(MICROSECOND, v_batch_start, v_batch_end)/1000
    );
    
    SET v_counter = v_counter + 100;
  END WHILE;
END$$
DELIMITER ;
```

**Monitoring During Test:**
```sql
-- Real-time monitoring query
SELECT 
  COUNT(DISTINCT test_id) as active_threads,
  COUNT(*) as total_batches,
  AVG(metric_value) as avg_latency_ms,
  MAX(metric_value) as max_latency_ms,
  MIN(metric_value) as min_latency_ms
FROM test_metrics
WHERE timestamp > NOW() - INTERVAL 10 SECOND;
```

**Scalability Analysis:**

| Threads | Total TPS | TPS/Thread | CPU Usage | Lock Waits |
|---------|-----------|------------|-----------|------------|
| 1 | ___ | ___ | ___% | ___ |
| 2 | ___ | ___ | ___% | ___ |
| 4 | ___ | ___ | ___% | ___ |
| 8 | ___ | ___ | ___% | ___ |
| 16 | ___ | ___ | ___% | ___ |
| 32 | ___ | ___ | ___% | ___ |

### Test 1.3: Sustained Insert Load Test

**Objective:** Verify system stability under sustained load

**Test Duration:** 1 hour

**Load Pattern:**
```
Minutes 0-15: Ramp up from 100 TPS to 1000 TPS
Minutes 15-45: Sustain 1000 TPS
Minutes 45-60: Ramp down to 100 TPS
```

**Monitoring Checkpoints (every 5 minutes):**
```sql
-- System metrics
SELECT 
  NOW() as checkpoint_time,
  (SELECT COUNT(*) FROM target_table) as total_records,
  (SELECT COUNT(*) FROM information_schema.processlist WHERE db = 'test') as active_connections,
  (SELECT SUM(data_length + index_length)/1024/1024 
   FROM information_schema.tables 
   WHERE table_schema = 'test') as database_size_mb;

-- Performance metrics
SELECT 
  COUNT(*) as operations_last_minute,
  AVG(metric_value) as avg_latency,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) as p95_latency,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) as p99_latency
FROM test_metrics
WHERE timestamp > NOW() - INTERVAL 1 MINUTE;
```

---

## Test Suite 2: Query Performance Tests

### Test 2.1: Index Effectiveness Test

**Objective:** Verify indexes are used efficiently

**Test Queries:**

1. **Primary Key Lookup:**
```sql
SET @id = 1000000;
SET profiling = 1;

SELECT * FROM target_table WHERE id = @id;

SHOW PROFILE;
SHOW STATUS LIKE 'Handler_read%';
```
Expected: < 1ms, Handler_read_key = 1

2. **Date Range Query (Indexed):**
```sql
SELECT COUNT(*) 
FROM target_table 
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY);

EXPLAIN FORMAT=JSON 
SELECT COUNT(*) 
FROM target_table 
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY);
```
Expected: < 10ms for 100K records/day

3. **Cursor Pagination:**
```sql
SET @last_id = 0;

-- First page
SELECT * FROM target_table 
WHERE id > @last_id 
ORDER BY id 
LIMIT 1000;

-- Measure subsequent pages
```
Expected: Consistent < 5ms per page

### Test 2.2: Complex Query Performance

**Test Queries with Increasing Complexity:**

1. **Aggregation Query:**
```sql
SELECT 
  DATE(created_at) as day,
  COUNT(*) as records,
  SUM(amount) as total,
  AVG(amount) as average,
  MIN(amount) as minimum,
  MAX(amount) as maximum
FROM target_table
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY DATE(created_at)
ORDER BY day DESC;
```

2. **Multi-Condition Filter:**
```sql
SELECT * FROM target_table
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND status IN ('pending', 'processing')
  AND amount > 100
  AND customer_id IN (SELECT id FROM customers WHERE vip = 1)
LIMIT 1000;
```

3. **Join Performance (if applicable):**
```sql
SELECT 
  t1.*,
  COUNT(t2.id) as related_count
FROM target_table t1
LEFT JOIN related_table t2 ON t1.id = t2.parent_id
WHERE t1.created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
GROUP BY t1.id
LIMIT 100;
```

**Performance Matrix:**

| Query Type | Data Volume | Expected Time | Actual Time | Rows Examined |
|------------|------------|---------------|-------------|---------------|
| PK Lookup | Any | < 1ms | ___ | 1 |
| Date Range (1 day) | 100K | < 10ms | ___ | ___ |
| Date Range (7 days) | 700K | < 50ms | ___ | ___ |
| Date Range (30 days) | 3M | < 200ms | ___ | ___ |
| Aggregation (30 days) | 3M | < 500ms | ___ | ___ |
| Complex Filter | 700K | < 100ms | ___ | ___ |

### Test 2.3: Concurrent Query Load

**Objective:** Test query performance under concurrent read load

**Test Setup:**
- 10 threads executing different query types
- Each thread runs for 5 minutes
- Mix of query types: 40% PK lookups, 30% date range, 20% aggregations, 10% complex

**Query Distribution:**
```sql
-- Thread 1-4: PK Lookups
DELIMITER $$
CREATE PROCEDURE pk_lookup_load()
BEGIN
  DECLARE v_counter INT DEFAULT 0;
  WHILE v_counter < 1000 DO
    SELECT * FROM target_table 
    WHERE id = FLOOR(RAND() * 1000000) + 1;
    SET v_counter = v_counter + 1;
  END WHILE;
END$$
DELIMITER ;

-- Thread 5-7: Date Range Queries
-- Thread 8-9: Aggregations  
-- Thread 10: Complex Queries
```

---

## Test Suite 3: Stress Tests

### Test 3.1: Connection Pool Exhaustion

**Objective:** Test behavior when connection limit is reached

**Test Procedure:**
1. Set max_connections to 100
2. Open 95 connections and hold them
3. Attempt operations with remaining 5 connections
4. Gradually increase to 100, 101, 102...

**Monitoring:**
```sql
SHOW STATUS LIKE 'Max_used_connections';
SHOW STATUS LIKE 'Threads_connected';
SHOW STATUS LIKE 'Connection_errors%';
SHOW PROCESSLIST;
```

**Expected Behavior:**
- Graceful handling at limit
- Clear error messages when exceeded
- No data corruption
- Recovery when connections freed

### Test 3.2: Memory Pressure Test

**Objective:** Test system behavior under memory constraints

**Test Procedure:**
1. Allocate large result sets
2. Hold multiple large result sets in memory
3. Monitor memory usage

```sql
-- Create memory pressure
SELECT * FROM target_table 
ORDER BY RAND() 
LIMIT 1000000;  -- Force full table scan and sort

-- Monitor memory
SHOW STATUS LIKE 'Innodb_buffer_pool%';
SHOW ENGINE INNODB STATUS\G
```

### Test 3.3: Disk I/O Saturation

**Objective:** Test performance when disk I/O is saturated

**Test Procedure:**
1. Disable query cache: `SET GLOBAL query_cache_type = 0`
2. Force disk reads with cold buffer pool
3. Execute large table scans concurrently

**Monitoring Commands:**
```bash
# Linux I/O monitoring
iostat -x 1

# MySQL I/O metrics
mysql> SHOW STATUS LIKE 'Innodb_data_reads';
mysql> SHOW STATUS LIKE 'Innodb_data_writes';
mysql> SHOW STATUS LIKE 'Innodb_os_log_written';
```

---

## Test Suite 4: Endurance Tests

### Test 4.1: 24-Hour Stability Test

**Objective:** Verify system stability over extended period

**Workload Profile:**
```
Hour 0-6: Low load (100 TPS)
Hour 6-9: Ramp up (100->500 TPS)
Hour 9-17: Business hours (500-1000 TPS)
Hour 17-20: Peak load (1000-1500 TPS)
Hour 20-24: Ramp down (500->100 TPS)
```

**Hourly Checkpoints:**
```sql
CREATE TABLE hourly_checkpoint (
  hour INT PRIMARY KEY,
  timestamp TIMESTAMP,
  total_operations BIGINT,
  avg_latency_ms DECIMAL(10,3),
  p99_latency_ms DECIMAL(10,3),
  error_count INT,
  table_size_gb DECIMAL(10,2),
  connection_count INT,
  memory_usage_gb DECIMAL(10,2)
);
```

**Success Criteria:**
- [ ] No memory leaks (stable memory usage)
- [ ] No performance degradation over time
- [ ] Error rate < 0.01%
- [ ] P99 latency < 100ms throughout

### Test 4.2: Week-Long Soak Test

**Objective:** Identify long-term issues

**Simplified Workload:**
- Consistent 200 TPS
- Daily maintenance operations
- Weekly statistics collection

**Daily Health Check:**
```sql
-- Table growth analysis
SELECT 
  DATE(created_at) as date,
  COUNT(*) as daily_records,
  SUM(data_length)/1024/1024 as data_mb
FROM information_schema.tables t
JOIN target_table d ON DATE(d.created_at) = CURDATE()
GROUP BY DATE(created_at);

-- Fragmentation check
SELECT 
  table_name,
  ROUND(data_free/1024/1024, 2) as free_space_mb,
  ROUND(data_length/1024/1024, 2) as data_size_mb,
  ROUND((data_free/(data_length+data_free))*100, 2) as fragmentation_pct
FROM information_schema.tables
WHERE table_schema = 'test';
```

---

## Test Suite 5: Chaos/Failure Tests

### Test 5.1: Random Connection Drops

**Objective:** Test resilience to network issues

**Test Procedure:**
1. During normal operations (500 TPS)
2. Randomly kill connections every 10-60 seconds
3. Monitor recovery and data integrity

```bash
# Connection killer script
while true; do
  mysql -e "KILL CONNECTION_ID();" 
  sleep $((RANDOM % 50 + 10))
done
```

### Test 5.2: Resource Starvation

**Objective:** Test graceful degradation

**Scenarios:**
1. CPU saturation (run CPU-intensive queries)
2. Memory exhaustion (allocate large buffers)
3. Disk full (fill to 95%)
4. Network congestion (introduce latency)

**Monitoring:**
```sql
-- Track degradation
CREATE TABLE degradation_test (
  timestamp TIMESTAMP,
  scenario VARCHAR(50),
  operations_per_sec DECIMAL(10,2),
  avg_latency_ms DECIMAL(10,3),
  error_rate DECIMAL(5,2)
);
```

---

## Performance Test Report Template

```markdown
# Performance Test Report

**Test Date:** ___________
**Test Duration:** ___________
**Environment:** ___________

## Executive Summary
- Peak TPS Achieved: ___________
- Average Latency: ___________ms
- P99 Latency: ___________ms
- Error Rate: ___________%
- Recommendations: ___________

## Detailed Results

### Insert Performance
| Metric | Single Thread | Multi-Thread (8) | Target | Status |
|--------|--------------|------------------|--------|--------|
| TPS | ___ | ___ | 1000 | ⚠️/✅/❌ |
| Avg Latency | ___ms | ___ms | <10ms | ⚠️/✅/❌ |
| P99 Latency | ___ms | ___ms | <100ms | ⚠️/✅/❌ |

### Query Performance
| Query Type | Volume | Time | Target | Status |
|------------|--------|------|--------|--------|
| PK Lookup | Any | ___ms | <1ms | ⚠️/✅/❌ |
| Date Range (1d) | 100K | ___ms | <10ms | ⚠️/✅/❌ |
| Date Range (7d) | 700K | ___ms | <50ms | ⚠️/✅/❌ |
| Aggregation | 3M | ___ms | <500ms | ⚠️/✅/❌ |

### Stability Metrics
- 24-Hour Test: PASS/FAIL
- Memory Leak: None/Detected
- Performance Degradation: None/___% over time

### Bottlenecks Identified
1. ___________
2. ___________
3. ___________

### Optimization Recommendations
1. ___________
2. ___________
3. ___________

## Appendices
- A. Full test logs
- B. System metrics graphs
- C. Query execution plans
- D. Configuration used
```

---

## Critical Performance Thresholds

### Must Meet (Production Ready)
- Insert TPS: > 1000 (batch mode)
- Query Latency P95: < 50ms
- Query Latency P99: < 100ms
- Error Rate: < 0.1%
- 24-Hour Stability: PASS

### Should Meet (Optimal)
- Insert TPS: > 5000
- Query Latency P95: < 20ms
- Query Latency P99: < 50ms
- Error Rate: < 0.01%
- Linear scaling to 8 threads

### Nice to Have (Excellence)
- Insert TPS: > 10000
- Query Latency P95: < 10ms
- Query Latency P99: < 20ms
- Zero errors in 24 hours
- Linear scaling to 16 threads