# SingleShardMultiTableDateTest

## TEST GOALS
- Validate MULTI_TABLE mode with date-based partitioning
- Test automatic table creation for different dates
- Verify cross-table queries and operations
- Ensure proper table naming and management
- Test retention period configuration

## COVERAGE
- Single shard with MULTI_TABLE mode
- Daily table granularity (events_20250920 format)
- Automatic table creation on insert
- Cross-table date range queries
- Update operations across multiple tables
- Delete operations with date constraints
- Pagination across multiple daily tables
- Table retention and cleanup policies

## DESCRIPTION
This test suite validates the MULTI_TABLE repository mode where data is partitioned into separate physical tables based on date. Each day's data goes into its own table (e.g., events_20250920), and the repository transparently handles operations across multiple tables. This mode is ideal for time-series data with clear retention policies.

## PREREQUISITES
- MySQL 5.7+ or MySQL 8.0+
- Ability to create/drop databases and tables

## TEST METHODS
1. **testAutomaticTableCreation** - Verify tables created for different dates
2. **testCrossTableDateRangeQueries** - Query data across multiple daily tables
3. **testUpdateAcrossDailyTables** - Update records in different daily tables
4. **testDeleteByDateRange** - Delete operations with date constraints
5. **testPaginationAcrossTables** - Paginate results from multiple tables
6. **testRetentionPeriodEnforcement** - Validate retention boundary handling