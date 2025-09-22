Test Scenario: SMS with Multi-Table Daily and Hourly Native Partitioning. Another version to 
test to ensure we can retrieve 100% of the entities with findByIdAndPartitionColRange.

Requirements:
- Track SMS messages with id and createdAt timestamp
- Single shard configuration
- Date-based multi-table partitioning (one table per day)
- Hourly native partitioning within each daily table (24 partitions per table)
- Sharding key: createdAt (LocalDateTime)
- Database name: sms_shard_01
- Test period: 7 days of data
- Create 15 days of tables (7 days past + today + 7 days future)
- Insert 1 million SMS records distributed across 7 days
- Verify records are in correct daily tables
- Verify hourly partition distribution within each table
- Verify partition pruning works for time-range queries



Test 7: Comprehensive Entity Verification with findByIdAndPartitionColRange
- read the current max partition limit in multi-table max date at start
- clean up prev db and tables
- Store all 100,000 SMS records distributed across 7 days in HashMap<String, SmsRecord> first
- create all necessary tables to fit the records by grouping by dates with 24 hourly partition,
    report when table creation finished
- Store all 100,000 SMS records in HashMap<String, SmsRecord> during insertion
- For each SMS entity in HashMap:
  - Generate random date range with +/- offsets (seconds, minutes, hours, days, months)
  - 100% should be valid ranges containing the SMS timestamp for this test. we want to see we are able to retrieve
    100% of the entities successfully as they were written to the db. 
- separate the insert and retrieve in two parts
  - insert 100,000 and report (print to console) the time required
  - fetch in bulk of 10,000 records and print to console for reporting 
- Use findByIdAndPartitionColRange(id, startDate, endDate) for lookup (2nd part of test)
- Compare entire fetched entity with stored entity from HashMap
- Verify all fields match exactly (id, createdAt, senderNumber, recipientNumber, messageContent, etc.)
- Success metrics:
  - Valid ranges: Must fetch entity and match exactly with HashMap version
  - Invalid ranges: Must return null (entity exists but outside range)

Entity Fields:
- id: String (auto-generated)
- createdAt: LocalDateTime (sharding key)
- senderNumber: String
- recipientNumber: String
- messageContent: String
- messageType: String (SMS, MMS)
- deliveryStatus: String (SENT, DELIVERED, FAILED, PENDING)
- messageLength: Integer
- retryCount: Integer