# Split-Verse Test Suite

This directory contains all test classes for the Split-Verse framework. All tests use the `main` method for execution and comprehensive logging.

## Test Classes

### 1. **AnnotationEnforcementTest**
- **Purpose**: Validates annotation processing and entity validation
- **Coverage**: @Table, @Id, @Column, @ShardingKey annotations
- **Key Tests**: Entity metadata extraction, field mapping validation

### 2. **PartitionTypeValidationTest**
- **Purpose**: Tests partition type validation and enforcement
- **Coverage**: DATE_BASED partitioning, unsupported partition types
- **Key Tests**: Partition type validation, error handling for unsupported types

### 3. **RepositoryModeTest**
- **Purpose**: Tests both PARTITIONED and MULTI_TABLE repository modes
- **Coverage**: Mode selection through builder, table creation verification
- **Key Tests**: Partitioned mode, multi-table daily/hourly/monthly modes

### 4. **ShardingEntityEnforcementTest**
- **Purpose**: Validates ShardingEntity interface enforcement
- **Coverage**: String ID requirement, createdAt field requirement
- **Key Tests**: Interface implementation validation, compile-time safety

### 5. **SplitVerseBasicOperationsTest**
- **Purpose**: Comprehensive CRUD operation testing with single shard
- **Coverage**: Insert, query, update, date range queries, pagination
- **Key Tests**: Basic operations, partition pruning, SQL injection prevention
- **Output**: Generates `SplitVerseBasicOperationsTest_report.log`

### 6. **SplitVerseMultiShardTest**
- **Purpose**: Tests multi-shard distribution and parallel operations
- **Coverage**: Hash routing, shard distribution, parallel queries
- **Key Tests**: Data distribution across shards, fan-out queries, shard failure handling
- **Output**: Generates `SplitVerseMultiShardTest_report.log`

## Running Tests

### Individual Test Execution
```bash
# Run specific test
java -cp "target/classes:target/test-classes:$(mvn dependency:build-classpath -q)" \
  com.telcobright.splitverse.tests.SplitVerseBasicOperationsTest

# Run repository mode test
java -cp "target/classes:target/test-classes:$(mvn dependency:build-classpath -q)" \
  com.telcobright.splitverse.tests.RepositoryModeTest
```

### All Tests
```bash
# Compile and run all tests
mvn test-compile
for test in AnnotationEnforcementTest PartitionTypeValidationTest RepositoryModeTest \
           ShardingEntityEnforcementTest SplitVerseBasicOperationsTest SplitVerseMultiShardTest; do
  echo "Running $test..."
  java -cp "target/classes:target/test-classes:$(mvn dependency:build-classpath -q)" \
    com.telcobright.splitverse.tests.$test
done
```

## Test Requirements

- MySQL 8.0+ running on 127.0.0.1:3306
- Database user: root/123456
- Java 11+
- Maven dependencies compiled

## Test Coverage

- ✅ Entity validation and annotations
- ✅ Builder-only access pattern enforcement
- ✅ String ID enforcement
- ✅ Partition type validation
- ✅ PARTITIONED mode (single table with partitions)
- ✅ MULTI_TABLE mode (separate tables per period)
- ✅ Basic CRUD operations
- ✅ Date range queries with partition pruning
- ✅ Multi-shard distribution
- ✅ Parallel query execution
- ✅ Cursor-based pagination
- ✅ SQL injection prevention
- ✅ Null handling
- ✅ Connection pooling
- ✅ Shard failure handling

## Note

All tests are standalone executable classes using `public static void main()`. They do not use JUnit or other testing frameworks, providing direct control over test execution and detailed logging.