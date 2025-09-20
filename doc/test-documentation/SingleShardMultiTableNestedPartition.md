# SingleShardMultiTableDateTest

## TEST GOALS
- Validate MULTI_TABLE mode with date-based partitioning
- Test automatic table creation for different dates
- Verify tables are created with native partition for more efficient searching through partition pruning 
- E.g. for datewise sharding key, one partition for each hour. total 24 partitions 
- For other dataTypes, split-verse should create  20 partitions by default if not specified other values
    through the builder. 
- test with various data including date, long and int. make sure that data from multiple tables are inserted and 
    retrieved correctly. create separate tests for different data types
- make sure when tables are created, sharding key column is indexed. 
- perform the test with 7 days retention period
- insert enough sample data, then retrieve data and verify. test with date types.
- use 7 days retention period. check if we have 15 days table created. this needs to pass.

## PREREQUISITES
- MySQL 5.7+ or MySQL 8.0+
- Ability to create/drop databases and tables

