test scenaio:
- track sip call state with callid
- no dated or range partitioning, all hash based
- shard based on hash, test with one shard only for now
- 2nd level partitioning in native-partition (NOT multi-table), partition based on hash
- dbname: sipcall_shard_01
- insert a thousand records
- retrieving records and verify count from db which table
- if possible, make advanced query to db to find out if record got inserted to right partition, by hash

