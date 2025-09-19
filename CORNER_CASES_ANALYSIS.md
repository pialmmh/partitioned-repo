# Corner Cases Analysis - Split-Verse Test Failures

## Summary
10 test failures remaining (7 failures + 3 errors) out of 71 tests

## Identified Corner Cases

### 1. **Pagination Across Multiple Shards**
**Test:** `MultiShardTest.testPaginationAcrossShards`
**Issue:** When paginating with `findBatchByIdGreaterThan()` across multiple shards, not all records are being returned
**Root Cause:** The pagination logic needs to merge and sort results from multiple shards correctly
**Expected:** Should retrieve at least 100 orders via pagination
**Actual:** Retrieving fewer records than expected

### 2. **Date Range Query with Events at Day Boundaries**
**Test:** `DatewiseMultiTableTest.testQueryAcrossMultipleDays`
**Issue:** Events inserted at specific times (with hours/minutes) may not be captured correctly in date range queries
**Root Cause:** The date range comparison might not handle time components correctly
**Expected:** Should find at least 5 events across 3 days
**Actual:** Finding fewer events

### 3. **Update Operations Across Shards with Date Ranges**
**Test:** `ThreeShardNestedPartitioningTest.testUpdateAcross3ShardsWithNestedPartitions`
**Error:** "No rows updated for ID: c2847065e40f474dbf06d7"
**Root Cause:** When updating by ID and date range across multiple shards, the record might be in a different shard or date range than expected

### 4. **Delete Operations with Date Range Filtering**
**Tests:**
- `ThreeShardNestedPartitioningTest.testDeleteAcross3ShardsWithNestedPartitions`
- `ComprehensiveStrategyTest.testDeleteOperations`
**Issue:** Entity not being deleted when using `deleteByIdAndPartitionRange`
**Root Cause:** The delete operation might not be finding the record in the correct table/shard combination

### 5. **Hourly/Daily Partition Boundary Queries**
**Tests:**
- `ComprehensiveStrategyTest.testDualKeyHashRange_DateTime_Daily`
- `ComprehensiveStrategyTest.testDualKeyHashRange_DateTime_Hourly`
**Issue:** Finding fewer entities than expected in date range queries with hourly/daily partitioning
**Root Cause:** Table name generation for hourly partitions might not align with query date ranges

### 6. **Concurrent Operations Under Load**
**Test:** `PerformanceTest.testConcurrentOperationsPerformance`
**Error:** SQLException during concurrent updates
**Root Cause:** Race conditions or deadlocks when multiple threads update the same records

### 7. **Query Performance Degradation**
**Test:** `PerformanceTest.testQueryPerformance`
**Issue:** Single record lookup taking longer than 20ms threshold
**Root Cause:** Missing indexes or inefficient table scanning across multiple tables

## Detailed Analysis

### Corner Case #1: Cross-Shard Pagination
The `findBatchByIdGreaterThan` method needs to:
1. Query each shard independently
2. Merge results maintaining ID order
3. Return exactly `batchSize` results
4. Handle edge case where some shards have no matching records

### Corner Case #2: Time-Aware Date Ranges
When querying date ranges:
1. Start date should be inclusive from 00:00:00
2. End date should be inclusive until 23:59:59
3. Table selection must consider full day boundaries

### Corner Case #3: Update with Partition Constraints
Updates with date ranges must:
1. Check all possible tables in the date range
2. Handle case where record's partition key doesn't match the provided range
3. Provide clear error when no matching record found

### Corner Case #4: Delete with Multiple Conditions
Delete operations need to:
1. Apply both ID and date range filters correctly
2. Check all relevant tables
3. Actually execute the DELETE (not just build the query)

### Corner Case #5: Partition Granularity Boundaries
For different partition granularities:
- **Hourly**: Table name includes hour (yyyyMMddHH)
- **Daily**: Table name includes day (yyyyMMdd)
- **Monthly**: Table name includes month (yyyyMM)
Must ensure date range queries span correct tables

### Corner Case #6: Concurrency Issues
Under concurrent load:
1. Connection pool exhaustion
2. Deadlocks on simultaneous updates
3. Race conditions in table creation

### Corner Case #7: Performance Optimization
Query performance issues:
1. Missing indexes on sharding columns
2. Full table scans when querying by ID
3. Inefficient UNION ALL across many tables

## Solutions Needed

1. **Fix pagination logic** - Implement proper merge-sort for cross-shard pagination
2. **Fix date range boundaries** - Ensure inclusive date ranges with proper time handling
3. **Fix update/delete with ranges** - Ensure operations check all relevant tables
4. **Fix partition table name generation** - Align table names with query logic
5. **Add retry logic** - Handle transient failures in concurrent scenarios
6. **Optimize queries** - Add appropriate indexes and query optimization

## Test Coverage
- **Passing:** 61 tests (86%)
- **Failing:** 10 tests (14%)
- **Core functionality:** Working correctly (BasicFunctionalityTest passes 100%)
- **Edge cases:** Need fixes for complex multi-shard and time-boundary scenarios