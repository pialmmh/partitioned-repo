# SingleShardNativePartitionTest

## TEST GOALS
Comprehensive test for single-shard with native MySQL partitioning

## COVERAGE
- SINGLE_TABLE mode with MySQL PARTITION BY RANGE
- Native partition creation and management
- Partition pruning optimization
- Query performance with native partitions
- Partition maintenance operations

## DESCRIPTION
This tests the PARTITIONED repository mode where a single table uses MySQL's native partitioning feature. The table is created with PARTITION BY RANGE clause, and MySQL handles partition management internally. This mode is ideal when you want to leverage database-native partition optimization features.

## PREREQUISITES
- MySQL 5.7+ with partitioning support enabled
- Sufficient privileges for partition DDL operations

## TEST METHODS
1. **testNativePartitionCreation** - Verify native partition table creation
2. **testPartitionPruning** - Validate MySQL partition pruning in queries
3. **testInsertIntoPartitions** - Test data routing to correct partitions
4. **testCrossPartitionQueries** - Query across multiple partitions
5. **testPartitionMaintenance** - Test adding/dropping partitions
6. **testPartitionInfo** - Verify partition metadata queries