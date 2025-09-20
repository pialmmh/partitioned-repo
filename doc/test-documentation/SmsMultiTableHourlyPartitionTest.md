Test Scenario: SMS with Multi-Table Daily and Hourly Native Partitioning

Requirements:
- Track SMS messages with id and createdAt timestamp
- Single shard configuration
- Date-based multi-table partitioning (one table per day)
- Hourly native partitioning within each daily table (24 partitions per table)
- Sharding key: createdAt (LocalDateTime)
- Database name: sms_shard_01
- Test period: 7 days of data
- Create 15 days of tables (7 days past + today + 7 days future)
- Insert 10,000 SMS records distributed across 7 days
- Verify records are in correct daily tables
- Verify hourly partition distribution within each table
- Verify partition pruning works for time-range queries

Test 7: Comprehensive Entity Verification with findByIdAndPartitionColRange
- Store all 10,000 SMS records in HashMap<String, SmsRecord> during insertion
- For each SMS entity in HashMap:
  - Generate random date range with +/- offsets (seconds, minutes, hours, days, months)
  - 80% should be valid ranges containing the SMS timestamp
  - 20% should be invalid ranges not containing the timestamp
- Use findByIdAndPartitionColRange(id, startDate, endDate) for lookup
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