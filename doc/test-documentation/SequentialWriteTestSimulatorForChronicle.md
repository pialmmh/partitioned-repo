# Sequential Write Test Simulator for Chronicle - Test Documentation

## Overview

The `SequentialWriteTestSimulatorForChronicle` test class simulates a Chronicle Queue-like client that manages its own sequential IDs and validates the integrity of sequential writes and retrievals at scale. This test demonstrates how external systems with their own ID management can integrate with split-verse's `SimpleSequentialRepository`.

## Test Purpose

Chronicle Queue is a high-performance, low-latency persistence library that maintains strict message ordering. This test simulates similar behavior by:

1. **Client-Managed Sequential IDs**: Events use externally generated sequential IDs (not managed by split-verse)
2. **Large-Scale Testing**: Writes 100,000 records to validate performance at scale
3. **Arbitrary Starting Point Retrieval**: Verifies retrieval from any sequence number with configurable batch sizes
4. **Data Integrity Verification**: Validates sequence continuity and checksums for each record

## Test Architecture

### Key Components

```
┌─────────────────────────────┐
│   Chronicle-like Client     │
│  (Sequence Generator)       │
│   Starting: 1,000,000       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SimpleSequentialRepository  │
│   - allowClientIds: true    │
│   - maxId: Long.MAX_VALUE   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│    MySQL Database           │
│  chronicle_events table     │
│  (Partitioned by date)      │
└─────────────────────────────┘
```

### Entity Structure

The `ChronicleEvent` entity simulates a Chronicle Queue message:

```java
- id: String (stores sequence number as string)
- sequenceNumber: long (1,000,000+)
- timestamp: LocalDateTime (sharding key)
- eventType: String (e.g., "MARKET_DATA")
- payload: String (JSON market data)
- checksum: long (data integrity validation)
```

## Test Scenarios

### 1. Sequential Write and Retrieval (Main Test)

**Test Method**: `testSequentialWriteAndRetrieval()`

#### Phase 1: Write Operations
- Writes 100,000 sequential events
- Each event has a unique sequence number starting from 1,000,000
- Uses `insertWithClientId()` to maintain external sequence
- Reports progress every 10,000 records
- Measures write throughput

#### Phase 2: Retrieval Verification
Tests multiple retrieval scenarios:

| Test Case | Starting Sequence | Batch Size | Expected Count | Description |
|-----------|------------------|------------|----------------|-------------|
| 1 | 1,000,000 | 1,000 | 1,000 | From beginning |
| 2 | 1,050,000 | 500 | 500 | From middle |
| 3 | 1,099,900 | 100 | 100 | From near end |
| 4 | 1,025,000 | 10,000 | 10,000 | Large batch |
| 5 | 1,005,000 | 10 pages × 100 | 1,000 | Pagination test |

#### Phase 3: Performance Testing
- Runs 10 iterations of random range reads
- Each iteration reads 1,000 records from random starting points
- Measures average read time and throughput

#### Phase 4: Data Integrity Verification
- Verifies total record count matches expected (100,000)
- Validates sequence range (1,000,000 to 1,099,999)
- Random samples 100 records for detailed verification
- Checks checksums for data integrity

### 2. Boundary and Edge Case Testing

**Test Method**: `testBoundaryConditions()`

| Test | Scenario | Expected Behavior |
|------|----------|------------------|
| Exact Boundaries | Retrieve first/last record only | Returns exactly 1 record |
| Over-Request | Request 20 records when only 10 remain | Returns available 10 |
| Beyond Range | Request from sequence beyond max | Returns empty list |
| Invalid Batch | Zero or negative batch size | Returns empty list |

### 3. Concurrent Operations Testing

**Test Method**: `testConcurrentOperations()`

- **Writer Thread**: Continuously writes 5,000 new events
- **Reader Thread**: Polls for new events every 500ms
- Validates data consistency during concurrent access
- Ensures readers see writes in correct sequence order

## Key Methods

### `retrieveEventsFromSequence(long startSequence, int batchSize)`

Simple batch retrieval starting from a specific sequence number:

```java
// Direct retrieval using findByIdRange
// startSequence=1000, batchSize=100 returns IDs 1000-1099
List<Event> events = repository.findByIdRange(startSequence, batchSize);
```

Key points:
- No cursor complexity - just startId + batchSize
- Returns records with IDs in range [startId, startId+batchSize-1]
- Missing records are skipped (returns only existing records)
- Simple and efficient for sequential access patterns

### `verifyRetrieval(...)`

Validates retrieval correctness:

```java
1. Retrieves events using specified parameters
2. Verifies count matches expected
3. Validates sequence continuity (no gaps)
4. Checks checksum integrity for each record
5. Reports success/failure
```

### `verifyPagination(...)`

Tests pagination across multiple pages:

```java
1. Retrieves data in small pages
2. Accumulates results across pages
3. Verifies total count and sequence continuity
4. Ensures no records lost between pages
```

## Performance Metrics

The test tracks and reports:

1. **Write Performance**
   - Total write time for 100K records
   - Write throughput (records/second)

2. **Read Performance**
   - Average read time for 1,000 records
   - Read throughput (records/second)
   - Pagination overhead

3. **Data Integrity**
   - Sequence continuity validation
   - Checksum verification
   - Boundary condition handling

## Expected Results

### Successful Test Indicators

✅ **Write Phase**
- 100,000 records written successfully
- Write rate > 10,000 records/second (typical)
- Sequence range: 1,000,000 to 1,099,999

✅ **Retrieval Phase**
- All test cases retrieve correct count
- Sequence continuity maintained (no gaps)
- All checksums validate correctly

✅ **Performance Phase**
- Read latency < 50ms for 1,000 records
- Throughput > 20,000 records/second

✅ **Integrity Phase**
- Total count = 100,000
- Min sequence = 1,000,000
- Max sequence = 1,099,999
- 100% checksum validation rate

### Sample Output

```
=== Chronicle Sequential Write Test Simulator ===
Database: chronicle_test_db
Total Records: 100000
Starting Sequence: 1000000
================================================

PHASE 1: Writing 100000 sequential events...
  Progress: 10000/100000 events written
  Progress: 20000/100000 events written
  ...
✓ Write completed in 8234ms
  - Records written: 100000
  - Write rate: 12144 records/sec
  - Sequence range: 1000000 to 1099999

PHASE 2: Verifying retrieval with arbitrary starting points...

Test Case: from beginning
  Starting sequence: 1000000
  Batch size: 1000
  ✓ Retrieved correct count: 1000
  ✓ Sequence continuity verified
  ✓ Checksums verified

Test Case: from middle
  Starting sequence: 1050000
  Batch size: 500
  ✓ Retrieved correct count: 500
  ✓ Sequence continuity verified
  ✓ Checksums verified

...

PHASE 3: Performance test - rapid sequential reads
  Average read time for 1000 records: 42.35ms
  Read throughput: 23623 records/sec
  ✓ Performance test completed

PHASE 4: Data integrity verification
  ✓ Total record count verified: 100000
  ✓ Sequence range verified: 1000000 to 1099999
  ✓ Random sample verification passed (100 records)
```

## Use Cases

This test simulates real-world scenarios for:

1. **Financial Market Data Systems**
   - High-frequency trading events
   - Order book updates
   - Price tick data

2. **Event Sourcing Systems**
   - Audit logs with strict ordering
   - Transaction journals
   - Command/event stores

3. **Message Queue Systems**
   - Chronicle Queue replacement
   - Kafka-like sequential messaging
   - Event streaming platforms

4. **Time-Series Databases**
   - Sensor data collection
   - Metrics and monitoring
   - IoT event streams

## Configuration Parameters

| Parameter | Default Value | Description |
|-----------|--------------|-------------|
| TOTAL_RECORDS | 100,000 | Number of records to write |
| STARTING_SEQUENCE | 1,000,000 | First sequence number |
| HOST | 127.0.0.1 | MySQL host |
| PORT | 3306 | MySQL port |
| TEST_DB | chronicle_test_db | Test database name |
| Retention Days | 30 | Data retention period |

## Running the Test

### Prerequisites

1. MySQL server running on localhost:3306
2. User credentials: root/123456
3. Sufficient disk space for 100K records

### Execution

```bash
# Run the full test suite
mvn test -Dtest=SequentialWriteTestSimulatorForChronicle

# Run specific test method
mvn test -Dtest=SequentialWriteTestSimulatorForChronicle#testSequentialWriteAndRetrieval

# Run with custom parameters (via system properties)
mvn test -Dtest=SequentialWriteTestSimulatorForChronicle \
  -Dtest.records=50000 \
  -Dtest.starting.sequence=2000000
```

## Troubleshooting

### Common Issues

1. **"Maximum ID reached" Error**
   - Cause: Sequence exceeded Long.MAX_VALUE
   - Solution: Reset starting sequence or enable wraparound

2. **Slow Write Performance**
   - Cause: Small connection pool or network latency
   - Solution: Increase pool size, check network

3. **Sequence Gaps Detected**
   - Cause: Failed transactions or concurrent modifications
   - Solution: Check for transaction rollbacks, ensure single writer

4. **Checksum Mismatches**
   - Cause: Data corruption or calculation errors
   - Solution: Verify checksum algorithm, check for data modifications

## Best Practices

1. **ID Management**
   - Always use client-managed IDs for external systems
   - Ensure IDs are strictly sequential for optimal performance
   - Consider ID ranges for distributed systems

2. **Batch Sizes**
   - Use 1000-5000 for optimal throughput
   - Smaller batches (100-500) for real-time requirements
   - Adjust based on memory constraints

3. **Performance Tuning**
   - Increase connection pool for concurrent access
   - Use bulk operations where possible
   - Consider partition pruning for date-range queries

4. **Data Integrity**
   - Always validate checksums in production
   - Implement retry logic for transient failures
   - Monitor sequence gaps as early warning signs

## Conclusion

This test demonstrates that split-verse's `SimpleSequentialRepository` can effectively handle Chronicle Queue-like workloads with:

- High-throughput sequential writes (>10K/sec)
- Low-latency reads (<50ms for 1K records)
- Perfect sequence ordering preservation
- Robust data integrity validation
- Flexible retrieval from any starting point

The test validates that external systems with their own ID management can successfully integrate with split-verse while maintaining their sequential guarantees.