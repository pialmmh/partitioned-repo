# Data Integrity Verification Procedures

## Comprehensive Data Integrity Framework

### Integrity Verification Levels

1. **Level 1: Basic Integrity** - Row counts, ID sequences
2. **Level 2: Structural Integrity** - Schema validation, constraints
3. **Level 3: Business Logic Integrity** - Data relationships, rules
4. **Level 4: Cross-System Integrity** - Multi-table/partition consistency
5. **Level 5: Temporal Integrity** - Time-based data accuracy

---

## Pre-Test Data Integrity Setup

### Create Verification Infrastructure

```sql
CREATE DATABASE IF NOT EXISTS integrity_check;
USE integrity_check;

-- Master verification tracking table
CREATE TABLE verification_results (
  check_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  check_type VARCHAR(50),
  table_name VARCHAR(100),
  check_description TEXT,
  expected_result TEXT,
  actual_result TEXT,
  status ENUM('PASS', 'FAIL', 'WARNING'),
  details JSON
);

-- Checksum tracking for data comparison
CREATE TABLE data_checksums (
  table_name VARCHAR(100),
  partition_name VARCHAR(100),
  record_count BIGINT,
  checksum_value VARCHAR(64),
  calculation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (table_name, partition_name)
);

-- Anomaly detection table
CREATE TABLE data_anomalies (
  anomaly_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  table_name VARCHAR(100),
  anomaly_type VARCHAR(50),
  affected_records INT,
  details JSON,
  resolution_status ENUM('PENDING', 'INVESTIGATING', 'RESOLVED', 'FALSE_POSITIVE')
);
```

---

## Test Suite 1: Primary Key Integrity

### Test 1.1: ID Uniqueness Verification

**Objective:** Ensure no duplicate primary keys exist

**Multi-Table Verification:**
```sql
-- For multi-table repositories
DELIMITER $$
CREATE PROCEDURE check_id_uniqueness_multi_table(IN table_prefix VARCHAR(50))
BEGIN
  DECLARE v_table_name VARCHAR(100);
  DECLARE v_done INT DEFAULT 0;
  DECLARE v_total_ids INT DEFAULT 0;
  DECLARE v_unique_ids INT DEFAULT 0;
  DECLARE table_cursor CURSOR FOR 
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = DATABASE() 
      AND table_name LIKE CONCAT(table_prefix, '_%');
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
  
  CREATE TEMPORARY TABLE IF NOT EXISTS temp_all_ids (
    id BIGINT,
    table_source VARCHAR(100),
    KEY idx_id (id)
  );
  
  OPEN table_cursor;
  
  read_loop: LOOP
    FETCH table_cursor INTO v_table_name;
    IF v_done THEN
      LEAVE read_loop;
    END IF;
    
    SET @sql = CONCAT('INSERT INTO temp_all_ids SELECT id, ''', 
                      v_table_name, ''' FROM ', v_table_name);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END LOOP;
  
  CLOSE table_cursor;
  
  -- Check for duplicates
  SELECT 
    COUNT(*) as total_ids,
    COUNT(DISTINCT id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT id) as duplicate_count
  INTO v_total_ids, v_unique_ids, @duplicate_count
  FROM temp_all_ids;
  
  -- Log results
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status)
  VALUES (
    'ID_UNIQUENESS',
    table_prefix,
    'Check for duplicate IDs across all tables',
    'duplicate_count = 0',
    CONCAT('duplicate_count = ', @duplicate_count),
    IF(@duplicate_count = 0, 'PASS', 'FAIL')
  );
  
  -- If duplicates found, get details
  IF @duplicate_count > 0 THEN
    SELECT 
      id,
      GROUP_CONCAT(table_source) as found_in_tables,
      COUNT(*) as occurrence_count
    FROM temp_all_ids
    GROUP BY id
    HAVING COUNT(*) > 1;
  END IF;
  
  DROP TEMPORARY TABLE temp_all_ids;
END$$
DELIMITER ;
```

**Single Partitioned Table Verification:**
```sql
-- For partitioned tables
DELIMITER $$
CREATE PROCEDURE check_id_uniqueness_partitioned(IN p_table_name VARCHAR(100))
BEGIN
  DECLARE v_duplicate_count INT;
  
  SET @sql = CONCAT('
    SELECT COUNT(*) - COUNT(DISTINCT id) INTO @dup_count
    FROM ', p_table_name
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status)
  VALUES (
    'ID_UNIQUENESS',
    p_table_name,
    'Check for duplicate IDs in partitioned table',
    'duplicate_count = 0',
    CONCAT('duplicate_count = ', @dup_count),
    IF(@dup_count = 0, 'PASS', 'FAIL')
  );
  
  -- Show duplicates if any
  IF @dup_count > 0 THEN
    SET @sql = CONCAT('
      SELECT id, COUNT(*) as occurrences
      FROM ', p_table_name, '
      GROUP BY id
      HAVING COUNT(*) > 1'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;
```

### Test 1.2: ID Sequence Continuity

**Objective:** Check for gaps in AUTO_INCREMENT sequence

```sql
DELIMITER $$
CREATE PROCEDURE check_id_sequence(IN p_table_name VARCHAR(100))
BEGIN
  DECLARE v_min_id, v_max_id, v_count, v_expected_count BIGINT;
  DECLARE v_gap_count INT;
  
  SET @sql = CONCAT('
    SELECT MIN(id), MAX(id), COUNT(*)
    INTO @min_id, @max_id, @count
    FROM ', p_table_name
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  SET v_expected_count = @max_id - @min_id + 1;
  SET v_gap_count = v_expected_count - @count;
  
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status, details)
  VALUES (
    'ID_SEQUENCE',
    p_table_name,
    'Check for gaps in ID sequence',
    'Continuous sequence or explicable gaps',
    CONCAT('Gaps found: ', v_gap_count),
    IF(v_gap_count = 0, 'PASS', 'WARNING'),
    JSON_OBJECT(
      'min_id', @min_id,
      'max_id', @max_id,
      'actual_count', @count,
      'expected_count', v_expected_count,
      'gap_count', v_gap_count,
      'gap_percentage', ROUND(v_gap_count * 100.0 / v_expected_count, 2)
    )
  );
  
  -- Find specific gaps if they exist
  IF v_gap_count > 0 AND v_gap_count < 1000 THEN
    SET @sql = CONCAT('
      SELECT 
        id + 1 as gap_start,
        (SELECT MIN(id) - 1 FROM ', p_table_name, ' t2 
         WHERE t2.id > t1.id) as gap_end,
        (SELECT MIN(id) - 1 FROM ', p_table_name, ' t2 
         WHERE t2.id > t1.id) - id as gap_size
      FROM ', p_table_name, ' t1
      WHERE NOT EXISTS (
        SELECT 1 FROM ', p_table_name, ' t2 
        WHERE t2.id = t1.id + 1
      )
      AND id < (SELECT MAX(id) FROM ', p_table_name, ')
      LIMIT 20'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;
```

---

## Test Suite 2: Temporal Integrity

### Test 2.1: Timestamp Consistency

**Objective:** Verify timestamps are logically consistent

```sql
DELIMITER $$
CREATE PROCEDURE check_timestamp_consistency(IN p_table_name VARCHAR(100))
BEGIN
  DECLARE v_future_count, v_ancient_count INT;
  DECLARE v_min_date, v_max_date DATETIME;
  
  -- Check for future dates
  SET @sql = CONCAT('
    SELECT COUNT(*) INTO @future_count
    FROM ', p_table_name, '
    WHERE created_at > NOW()'
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Check for unreasonably old dates (before system deployment)
  SET @sql = CONCAT('
    SELECT COUNT(*) INTO @ancient_count
    FROM ', p_table_name, '
    WHERE created_at < ''2020-01-01'''
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Get date range
  SET @sql = CONCAT('
    SELECT MIN(created_at), MAX(created_at)
    INTO @min_date, @max_date
    FROM ', p_table_name
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status, details)
  VALUES (
    'TIMESTAMP_CONSISTENCY',
    p_table_name,
    'Check timestamp logical consistency',
    'No future dates, no ancient dates',
    CONCAT('Future: ', @future_count, ', Ancient: ', @ancient_count),
    IF(@future_count = 0 AND @ancient_count = 0, 'PASS', 'FAIL'),
    JSON_OBJECT(
      'future_records', @future_count,
      'ancient_records', @ancient_count,
      'min_timestamp', @min_date,
      'max_timestamp', @max_date,
      'date_range_days', DATEDIFF(@max_date, @min_date)
    )
  );
  
  -- Show problematic records
  IF @future_count > 0 THEN
    SET @sql = CONCAT('
      SELECT id, created_at, ''FUTURE'' as issue
      FROM ', p_table_name, '
      WHERE created_at > NOW()
      LIMIT 10'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;
```

### Test 2.2: Partition/Table Date Alignment

**Objective:** Verify data is in correct partition/table based on timestamp

```sql
-- For Multi-Table
DELIMITER $$
CREATE PROCEDURE check_table_date_alignment(IN table_prefix VARCHAR(50))
BEGIN
  DECLARE v_table_name VARCHAR(100);
  DECLARE v_expected_date DATE;
  DECLARE v_misaligned_count INT;
  DECLARE v_done INT DEFAULT 0;
  DECLARE table_cursor CURSOR FOR 
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = DATABASE() 
      AND table_name REGEXP CONCAT('^', table_prefix, '_[0-9]{8}$');
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
  
  OPEN table_cursor;
  
  read_loop: LOOP
    FETCH table_cursor INTO v_table_name;
    IF v_done THEN
      LEAVE read_loop;
    END IF;
    
    -- Extract expected date from table name (format: prefix_YYYYMMDD)
    SET v_expected_date = STR_TO_DATE(
      SUBSTRING(v_table_name, LENGTH(table_prefix) + 2),
      '%Y%m%d'
    );
    
    -- Check for misaligned records
    SET @sql = CONCAT('
      SELECT COUNT(*) INTO @misaligned
      FROM ', v_table_name, '
      WHERE DATE(created_at) != ''', v_expected_date, ''''
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    IF @misaligned > 0 THEN
      INSERT INTO data_anomalies 
      (table_name, anomaly_type, affected_records, details)
      VALUES (
        v_table_name,
        'DATE_MISALIGNMENT',
        @misaligned,
        JSON_OBJECT(
          'expected_date', v_expected_date,
          'misaligned_count', @misaligned
        )
      );
      
      -- Show sample of misaligned records
      SET @sql = CONCAT('
        SELECT id, created_at, ''', v_expected_date, ''' as expected_date
        FROM ', v_table_name, '
        WHERE DATE(created_at) != ''', v_expected_date, '''
        LIMIT 5'
      );
      
      PREPARE stmt FROM @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
    END IF;
    
  END LOOP;
  
  CLOSE table_cursor;
END$$
DELIMITER ;
```

---

## Test Suite 3: Referential Integrity

### Test 3.1: Cross-Table/Partition References

**Objective:** Verify foreign key relationships work across tables/partitions

```sql
DELIMITER $$
CREATE PROCEDURE check_referential_integrity(
  IN parent_table VARCHAR(100),
  IN child_table VARCHAR(100),
  IN parent_key VARCHAR(50),
  IN child_key VARCHAR(50)
)
BEGIN
  DECLARE v_orphaned_count INT;
  DECLARE v_total_child_records INT;
  
  -- Find orphaned records
  SET @sql = CONCAT('
    SELECT COUNT(*) INTO @orphaned
    FROM ', child_table, ' c
    WHERE NOT EXISTS (
      SELECT 1 FROM ', parent_table, ' p
      WHERE p.', parent_key, ' = c.', child_key, '
    )'
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Get total child records
  SET @sql = CONCAT('
    SELECT COUNT(*) INTO @total_child
    FROM ', child_table
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status, details)
  VALUES (
    'REFERENTIAL_INTEGRITY',
    CONCAT(child_table, '->', parent_table),
    'Check for orphaned records',
    'No orphaned records',
    CONCAT('Orphaned: ', @orphaned, ' of ', @total_child),
    IF(@orphaned = 0, 'PASS', 'FAIL'),
    JSON_OBJECT(
      'parent_table', parent_table,
      'child_table', child_table,
      'orphaned_records', @orphaned,
      'total_child_records', @total_child,
      'orphan_percentage', ROUND(@orphaned * 100.0 / @total_child, 2)
    )
  );
  
  -- Show orphaned records if any
  IF @orphaned > 0 AND @orphaned < 100 THEN
    SET @sql = CONCAT('
      SELECT c.*
      FROM ', child_table, ' c
      WHERE NOT EXISTS (
        SELECT 1 FROM ', parent_table, ' p
        WHERE p.', parent_key, ' = c.', child_key, '
      )
      LIMIT 10'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$
DELIMITER ;
```

---

## Test Suite 4: Data Consistency

### Test 4.1: Checksum Verification

**Objective:** Detect any data corruption or unauthorized modifications

```sql
DELIMITER $$
CREATE PROCEDURE calculate_data_checksum(IN p_table_name VARCHAR(100))
BEGIN
  DECLARE v_checksum VARCHAR(64);
  DECLARE v_record_count BIGINT;
  
  -- Calculate checksum of all data
  SET @sql = CONCAT('
    SELECT 
      MD5(GROUP_CONCAT(
        MD5(CONCAT_WS(''|'', 
          id, customer_id, amount, status, 
          DATE_FORMAT(created_at, ''%Y-%m-%d %H:%i:%s'')
        ))
        ORDER BY id
      )) INTO @checksum
    FROM ', p_table_name
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Get record count
  SET @sql = CONCAT('
    SELECT COUNT(*) INTO @count
    FROM ', p_table_name
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Store checksum for comparison
  INSERT INTO data_checksums 
  (table_name, partition_name, record_count, checksum_value)
  VALUES (p_table_name, 'FULL_TABLE', @count, @checksum)
  ON DUPLICATE KEY UPDATE
    record_count = @count,
    checksum_value = @checksum,
    calculation_time = NOW();
  
  SELECT 
    p_table_name as table_name,
    @count as records,
    @checksum as checksum,
    NOW() as calculated_at;
END$$
DELIMITER ;
```

### Test 4.2: Data Distribution Analysis

**Objective:** Verify data is evenly distributed (no hot spots)

```sql
DELIMITER $$
CREATE PROCEDURE analyze_data_distribution(IN p_table_name VARCHAR(100))
BEGIN
  -- Hourly distribution
  SET @sql = CONCAT('
    SELECT 
      HOUR(created_at) as hour,
      COUNT(*) as record_count,
      MIN(id) as min_id,
      MAX(id) as max_id,
      AVG(amount) as avg_amount
    FROM ', p_table_name, '
    GROUP BY HOUR(created_at)
    ORDER BY hour'
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Daily distribution (for multi-day data)
  SET @sql = CONCAT('
    SELECT 
      DATE(created_at) as date,
      COUNT(*) as record_count,
      STD(amount) as amount_std_dev,
      MIN(amount) as min_amount,
      MAX(amount) as max_amount
    FROM ', p_table_name, '
    GROUP BY DATE(created_at)
    ORDER BY date'
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Check for skewed distribution
  SET @sql = CONCAT('
    WITH distribution_stats AS (
      SELECT 
        DATE(created_at) as date,
        COUNT(*) as daily_count
      FROM ', p_table_name, '
      GROUP BY DATE(created_at)
    )
    SELECT 
      AVG(daily_count) as avg_daily_records,
      STD(daily_count) as std_dev,
      MIN(daily_count) as min_daily,
      MAX(daily_count) as max_daily,
      MAX(daily_count) / AVG(daily_count) as max_skew_ratio
    FROM distribution_stats'
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;
```

---

## Test Suite 5: Transaction Integrity

### Test 5.1: Batch Operation Atomicity

**Objective:** Verify batch operations are atomic (all or nothing)

```sql
DELIMITER $$
CREATE PROCEDURE test_batch_atomicity()
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    INSERT INTO verification_results 
    (check_type, table_name, check_description, expected_result, actual_result, status)
    VALUES (
      'BATCH_ATOMICITY',
      'test_table',
      'Test batch insert rollback on error',
      'Rollback successful',
      'Rollback triggered',
      'PASS'
    );
  END;
  
  START TRANSACTION;
  
  -- Insert batch with intentional error
  INSERT INTO test_table (id, amount) VALUES 
    (1001, 100.00),
    (1002, 200.00),
    (1003, 300.00),
    (NULL, 400.00), -- This will cause error
    (1005, 500.00);
  
  COMMIT;
  
  -- If we get here, atomicity failed
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status)
  VALUES (
    'BATCH_ATOMICITY',
    'test_table',
    'Test batch insert rollback on error',
    'Rollback on error',
    'Partial insert occurred',
    'FAIL'
  );
END$$
DELIMITER ;
```

### Test 5.2: Concurrent Modification Detection

**Objective:** Detect lost updates from concurrent modifications

```sql
DELIMITER $$
CREATE PROCEDURE test_concurrent_modifications()
BEGIN
  DECLARE v_initial_value, v_final_value DECIMAL(10,2);
  DECLARE v_expected_value DECIMAL(10,2);
  
  -- Setup: Create test record
  INSERT INTO test_table (id, amount, version) VALUES (9999, 1000.00, 1);
  
  -- Simulate concurrent modifications
  -- Process 1: Add 100
  UPDATE test_table 
  SET amount = amount + 100, version = version + 1
  WHERE id = 9999 AND version = 1;
  
  -- Process 2: Add 200 (should fail due to version mismatch)
  UPDATE test_table 
  SET amount = amount + 200, version = version + 1
  WHERE id = 9999 AND version = 1;
  
  -- Check final value
  SELECT amount INTO v_final_value
  FROM test_table WHERE id = 9999;
  
  SET v_expected_value = 1100.00; -- Only first update should succeed
  
  INSERT INTO verification_results 
  (check_type, table_name, check_description, expected_result, actual_result, status)
  VALUES (
    'CONCURRENT_MODIFICATION',
    'test_table',
    'Test optimistic locking',
    CONCAT('Final value = ', v_expected_value),
    CONCAT('Final value = ', v_final_value),
    IF(v_final_value = v_expected_value, 'PASS', 'FAIL')
  );
  
  -- Cleanup
  DELETE FROM test_table WHERE id = 9999;
END$$
DELIMITER ;
```

---

## Comprehensive Integrity Check Suite

### Master Integrity Check Procedure

```sql
DELIMITER $$
CREATE PROCEDURE run_full_integrity_check(
  IN p_table_prefix VARCHAR(50),
  IN p_check_level INT -- 1=Basic, 2=Standard, 3=Comprehensive
)
BEGIN
  DECLARE v_start_time TIMESTAMP DEFAULT NOW();
  DECLARE v_total_checks INT DEFAULT 0;
  DECLARE v_passed INT DEFAULT 0;
  DECLARE v_failed INT DEFAULT 0;
  DECLARE v_warnings INT DEFAULT 0;
  
  -- Clear previous results
  DELETE FROM verification_results 
  WHERE DATE(check_timestamp) = CURDATE();
  
  -- Level 1: Basic Checks
  CALL check_id_uniqueness_multi_table(p_table_prefix);
  CALL check_id_sequence(CONCAT(p_table_prefix, '_', DATE_FORMAT(NOW(), '%Y%m%d')));
  SET v_total_checks = v_total_checks + 2;
  
  IF p_check_level >= 2 THEN
    -- Level 2: Standard Checks
    CALL check_timestamp_consistency(CONCAT(p_table_prefix, '_', DATE_FORMAT(NOW(), '%Y%m%d')));
    CALL check_table_date_alignment(p_table_prefix);
    CALL analyze_data_distribution(CONCAT(p_table_prefix, '_', DATE_FORMAT(NOW(), '%Y%m%d')));
    SET v_total_checks = v_total_checks + 3;
  END IF;
  
  IF p_check_level >= 3 THEN
    -- Level 3: Comprehensive Checks
    CALL calculate_data_checksum(CONCAT(p_table_prefix, '_', DATE_FORMAT(NOW(), '%Y%m%d')));
    CALL test_batch_atomicity();
    CALL test_concurrent_modifications();
    SET v_total_checks = v_total_checks + 3;
  END IF;
  
  -- Calculate summary
  SELECT 
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END)
  INTO v_passed, v_failed, v_warnings
  FROM verification_results
  WHERE check_timestamp >= v_start_time;
  
  -- Display summary
  SELECT 
    'INTEGRITY CHECK SUMMARY' as report,
    v_total_checks as total_checks,
    v_passed as passed,
    v_failed as failed,
    v_warnings as warnings,
    ROUND(v_passed * 100.0 / v_total_checks, 2) as pass_rate,
    TIMESTAMPDIFF(SECOND, v_start_time, NOW()) as duration_seconds;
  
  -- Show failed checks
  IF v_failed > 0 THEN
    SELECT 
      check_type,
      table_name,
      check_description,
      actual_result,
      details
    FROM verification_results
    WHERE check_timestamp >= v_start_time
      AND status = 'FAIL';
  END IF;
  
  -- Show warnings
  IF v_warnings > 0 THEN
    SELECT 
      check_type,
      table_name,
      check_description,
      actual_result,
      details
    FROM verification_results
    WHERE check_timestamp >= v_start_time
      AND status = 'WARNING';
  END IF;
END$$
DELIMITER ;
```

---

## Integrity Monitoring Dashboard Queries

### Real-Time Integrity Metrics

```sql
-- Current integrity status
CREATE VIEW integrity_dashboard AS
SELECT 
  DATE(check_timestamp) as check_date,
  COUNT(*) as total_checks,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
  SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) as warnings,
  ROUND(
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
    2
  ) as pass_rate
FROM verification_results
GROUP BY DATE(check_timestamp)
ORDER BY check_date DESC;

-- Recent anomalies
CREATE VIEW recent_anomalies AS
SELECT 
  detection_time,
  table_name,
  anomaly_type,
  affected_records,
  resolution_status
FROM data_anomalies
WHERE detection_time > NOW() - INTERVAL 24 HOUR
ORDER BY detection_time DESC;

-- Data growth tracking
CREATE VIEW data_growth_metrics AS
SELECT 
  table_name,
  MIN(calculation_time) as first_check,
  MAX(calculation_time) as last_check,
  MIN(record_count) as initial_count,
  MAX(record_count) as current_count,
  MAX(record_count) - MIN(record_count) as growth,
  ROUND(
    (MAX(record_count) - MIN(record_count)) * 100.0 / MIN(record_count),
    2
  ) as growth_percentage
FROM data_checksums
GROUP BY table_name;
```

---

## Automated Integrity Alert Rules

```sql
-- Create alert triggers
DELIMITER $$
CREATE TRIGGER integrity_failure_alert
AFTER INSERT ON verification_results
FOR EACH ROW
BEGIN
  IF NEW.status = 'FAIL' THEN
    INSERT INTO integrity_alerts 
    (alert_time, alert_level, check_type, table_name, message)
    VALUES (
      NOW(),
      'CRITICAL',
      NEW.check_type,
      NEW.table_name,
      CONCAT('Integrity check failed: ', NEW.check_description)
    );
  END IF;
END$$
DELIMITER ;
```

---

## Integrity Verification Schedule

### Recommended Verification Frequency

| Check Type | Frequency | Automated | Manual Review |
|------------|-----------|-----------|---------------|
| ID Uniqueness | After each batch | Yes | On failure |
| Timestamp Consistency | Hourly | Yes | Daily |
| Table/Partition Alignment | Daily | Yes | Weekly |
| Referential Integrity | Daily | Yes | On change |
| Data Distribution | Weekly | Yes | Monthly |
| Full Checksum | Weekly | Yes | On suspicion |
| Transaction Atomicity | After deployment | No | Yes |

### Manual Verification Checklist

**Daily Checks:**
- [ ] Review integrity dashboard
- [ ] Check for new anomalies
- [ ] Verify data growth is within expected range
- [ ] Confirm no CRITICAL alerts

**Weekly Checks:**
- [ ] Run comprehensive integrity check
- [ ] Review data distribution reports
- [ ] Verify checksum consistency
- [ ] Analyze any WARNING status items

**Monthly Checks:**
- [ ] Full data audit
- [ ] Cross-system reconciliation
- [ ] Performance impact assessment
- [ ] Update integrity rules based on findings

---

## Integrity Recovery Procedures

### When Integrity Violations Are Detected

1. **Immediate Actions:**
   - Stop write operations if critical
   - Capture current state
   - Alert relevant teams

2. **Investigation:**
   - Identify scope of violation
   - Determine root cause
   - Assess data recovery options

3. **Recovery:**
   - Execute recovery procedure
   - Verify integrity restored
   - Document incident

4. **Prevention:**
   - Update integrity checks
   - Modify application logic if needed
   - Add monitoring for specific issue

### Recovery Scripts Template

```sql
-- Duplicate ID Recovery
DELIMITER $$
CREATE PROCEDURE recover_duplicate_ids(IN p_table_name VARCHAR(100))
BEGIN
  -- Identify duplicates
  -- Keep first occurrence, update others
  -- Reassign new IDs to duplicates
  -- Verify uniqueness restored
END$$
DELIMITER ;

-- Misaligned Data Recovery  
DELIMITER $$
CREATE PROCEDURE recover_misaligned_data(IN p_table_prefix VARCHAR(50))
BEGIN
  -- Identify misaligned records
  -- Move to correct table/partition
  -- Verify alignment restored
  -- Update checksums
END$$
DELIMITER ;
```