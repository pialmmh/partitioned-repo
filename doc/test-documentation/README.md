# Split-Verse Test Documentation

This directory contains documentation for all test classes in the Split-Verse framework. Each test file is documented with its goals, coverage, and test methods.

## Test Categories

### Core Functionality Tests
- [BasicFunctionalityTest](BasicFunctionalityTest.md) - Core CRUD operations and single-shard functionality
- [CrudOperationsTest](CrudOperationsTest.md) - Comprehensive CRUD operation testing
- [ApiValidationTest](ApiValidationTest.md) - API contract and builder validation

### Partitioning Tests
- [SingleShardMultiTableDateTest](SingleShardMultiTableDateTest.md) - MULTI_TABLE mode with date-based partitioning
- [SingleShardNativePartitionTest](SingleShardNativePartitionTest.md) - Native MySQL partitioning (PARTITIONED mode)
- [PartitionBoundaryTest](PartitionBoundaryTest.md) - MySQL-style partition boundary behavior

### Performance Tests
- [PerformanceTest](PerformanceTest.md) - High-volume operations and performance metrics

## Test Execution

### Running All Tests
```bash
mvn test
```

### Running Specific Test Class
```bash
mvn test -Dtest=TestClassName
```

### Running with Performance Metrics
```bash
mvn test -Dtest=PerformanceTest -DargLine="-Xmx2g"
```

## Test Requirements

### Database Setup
- MySQL 5.7+ or MySQL 8.0+
- User with CREATE/DROP database privileges
- Connection: 127.0.0.1:3306
- Credentials: root/123456

### Environment
- Java 21+
- Maven 3.8+
- Sufficient memory for performance tests (2GB+ recommended)

## Test Coverage Areas

### Functional Coverage
- Entity annotation processing
- Repository initialization
- CRUD operations (Create, Read, Update, Delete)
- Bulk operations
- Transaction handling
- Error handling and validation

### Partitioning Coverage
- Multi-table partitioning (separate tables per time period)
- Native database partitioning (single table with partitions)
- Partition boundary handling
- Cross-partition queries
- Retention management

### Performance Coverage
- Throughput testing (1000+ records/second)
- Query optimization with partition pruning
- Concurrent operation handling
- Memory efficiency
- Connection pool behavior

## Adding New Tests

When creating new test classes, document them following this template:

```markdown
# TestClassName

## TEST GOALS
- Main objectives of the test
- What functionality is being validated

## COVERAGE
- Specific areas covered by the test
- Components or features tested

## DESCRIPTION
Detailed description of what the test suite does and why it's important.

## PREREQUISITES
- Any specific requirements
- Database setup needs
- Configuration requirements

## TEST METHODS
1. **methodName** - Description of what this test validates
2. **methodName** - Description of what this test validates

## METRICS (if applicable)
- Performance metrics measured
- Success criteria
```

## Test Maintenance

### Regular Tasks
- Update test documentation when tests are modified
- Review and update test coverage quarterly
- Monitor test execution times
- Clean up test databases after failures

### Best Practices
- Keep tests independent and idempotent
- Use meaningful test data
- Clean up resources in @AfterAll methods
- Document any special setup requirements
- Use clear assertion messages