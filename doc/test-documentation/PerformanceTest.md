# PerformanceTest

## TEST GOALS
- Measure throughput for high-volume operations
- Analyze query performance with partition pruning
- Test scalability with large datasets (10K+ records)
- Validate batch operation efficiency
- Monitor memory usage and connection pooling

## COVERAGE
- Bulk insert performance (1000+ records/second)
- Query optimization with partition pruning
- Concurrent operation handling
- Memory efficiency for large result sets
- Connection pool behavior under load
- Index utilization and query planning

## DESCRIPTION
This performance test suite validates Split-Verse's ability to handle high-volume data operations efficiently. It measures throughput, latency, and resource utilization for various operations including bulk inserts, complex queries, and concurrent operations. The tests help identify performance bottlenecks and validate optimization strategies.

## PREREQUISITES
- MySQL 5.7+ or MySQL 8.0+ with proper configuration
- Sufficient memory for JVM (recommended: -Xmx2g)

## TEST METHODS
1. **testBulkInsertPerformance** - Measure bulk insert throughput
2. **testQueryPerformanceWithPartitionPruning** - Validate partition optimization
3. **testConcurrentOperations** - Test thread safety and concurrency
4. **testLargeResultSetHandling** - Memory efficiency for large queries
5. **testUpdatePerformance** - Bulk update operations
6. **testDeletePerformance** - Bulk delete operations

## METRICS
- Insert throughput: Records per second
- Query latency: Milliseconds per query
- Memory usage: Heap consumption patterns
- Connection pool: Active/idle connection counts
- CPU utilization: Processing overhead