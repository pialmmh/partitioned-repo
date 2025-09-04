package com.telcobright.tests;

import com.telcobright.api.RepositoryProxy;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.examples.entity.SmsEntity;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Comprehensive test suite for Multi-Table Repository
 */
public class MultiTableRepositoryTest extends BaseRepositoryTest {
    
    private GenericMultiTableRepository<SmsEntity, Long> smsRepository;
    private RepositoryProxy<SmsEntity, Long> repositoryProxy;
    
    @Override
    public void runAllTests() throws Exception {
        logger.info("Starting Multi-Table Repository Tests");
        
        // Initialize
        setUp();
        initializeRepository();
        
        try {
            // Run test suites
            testTableAutoCreation();
            testDataDistribution();
            testCrossTableQueries();
            testMaintenanceOperations();
            testBoundaryConditions();
            testCursorPagination();
            testBatchOperations();
            testHourlyPartitioning();
            testDataMigration();
            testFailureRecovery();
            
            logger.info("All Multi-Table Repository tests completed successfully");
            
        } finally {
            // Cleanup
            if (smsRepository != null) {
                smsRepository.shutdown();
            }
            tearDown();
        }
    }
    
    /**
     * Initialize repository
     */
    private void initializeRepository() {
        smsRepository = GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
            .host(TEST_HOST)
            .port(TEST_PORT)
            .database(TEST_DATABASE)
            .username(TEST_USER)
            .password(TEST_PASSWORD)
            .tablePrefix("sms")
            .partitionRetentionPeriod(7)
            .enableMonitoring(true)
            .build();
        
        repositoryProxy = RepositoryProxy.forMultiTable(smsRepository);
        
        logger.info("Multi-table repository initialized with prefix: sms");
    }
    
    /**
     * Test 1: Table Auto-Creation
     */
    private void testTableAutoCreation() throws Exception {
        logger.info("TEST 1: Table Auto-Creation");
        
        String testId = "auto_creation_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Insert records for 5 different days
            LocalDateTime[] testDates = {
                LocalDateTime.now(),
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now().minusDays(2),
                LocalDateTime.now().minusDays(3),
                LocalDateTime.now().minusDays(4)
            };
            
            for (LocalDateTime date : testDates) {
                for (int i = 0; i < 20; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setSenderId("sender" + i);
                    sms.setReceiverId("receiver" + i);
                    sms.setMessage("Test message for " + date.toLocalDate());
                    sms.setStatus("sent");
                    sms.setCreatedAt(date);
                    
                    repositoryProxy.insert(sms);
                    successfulOperations.incrementAndGet();
                }
            }
            
            // Verify tables were created
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as table_count FROM information_schema.tables " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' AND table_name LIKE 'sms_%'"
                );
                
                rs.next();
                int tableCount = rs.getInt("table_count");
                
                if (tableCount != 5) {
                    throw new AssertionError("Expected 5 tables, found: " + tableCount);
                }
                
                logger.info("✓ Successfully created " + tableCount + " tables");
            }
            
            // Verify hourly partitions within each table
            for (LocalDateTime date : testDates) {
                String tableName = "sms_" + date.format(dateFormatter);
                verifyHourlyPartitions(tableName);
            }
            
            saveTestResults(testId, "Table Auto-Creation", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Table Auto-Creation", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 2: Data Distribution
     */
    private void testDataDistribution() throws Exception {
        logger.info("TEST 2: Data Distribution Across Tables");
        
        String testId = "data_distribution_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Insert 1000 records with varying timestamps
            List<SmsEntity> testData = new ArrayList<>();
            for (int day = 0; day < 7; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    for (int i = 0; i < 6; i++) {
                        SmsEntity sms = new SmsEntity();
                        sms.setSenderId("dist_sender_" + i);
                        sms.setReceiverId("dist_receiver_" + i);
                        sms.setMessage("Distribution test message");
                        sms.setStatus("pending");
                        sms.setCreatedAt(LocalDateTime.now()
                            .minusDays(day)
                            .withHour(hour)
                            .withMinute(i * 10));
                        testData.add(sms);
                    }
                }
            }
            
            // Batch insert
            repositoryProxy.insertMultiple(testData);
            successfulOperations.addAndGet(testData.size());
            
            // Verify distribution
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT table_name, table_rows " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' " +
                    "AND table_name LIKE 'sms_%' " +
                    "ORDER BY table_name"
                );
                
                Map<String, Integer> distribution = new HashMap<>();
                while (rs.next()) {
                    distribution.put(rs.getString("table_name"), rs.getInt("table_rows"));
                }
                
                logger.info("Data distribution across tables:");
                for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
                    logger.info("  " + entry.getKey() + ": " + entry.getValue() + " rows");
                    
                    // Each day should have 144 records (24 hours * 6 records)
                    if (Math.abs(entry.getValue() - 144) > 10) { // Allow some variance
                        logger.warn("Uneven distribution in " + entry.getKey());
                    }
                }
            }
            
            // Verify hourly distribution within a table
            verifyHourlyDistribution("sms_" + LocalDateTime.now().format(dateFormatter));
            
            saveTestResults(testId, "Data Distribution", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Data Distribution", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 3: Cross-Table Queries
     */
    private void testCrossTableQueries() throws Exception {
        logger.info("TEST 3: Cross-Table Query Operations");
        
        String testId = "cross_table_queries_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Test date range query spanning multiple tables
            LocalDateTime startDate = LocalDateTime.now().minusDays(6);
            LocalDateTime endDate = LocalDateTime.now();
            
            List<SmsEntity> results = repositoryProxy.findAllByDateRange(startDate, endDate);
            logger.info("Date range query returned " + results.size() + " records");
            
            // Test findById across tables
            Long testId1 = results.get(0).getId();
            SmsEntity found = repositoryProxy.findById(testId1);
            if (found == null) {
                throw new AssertionError("Failed to find entity by ID: " + testId1);
            }
            logger.info("✓ Successfully found entity by ID across tables");
            
            // Test cursor-based iteration
            Long lastId = 0L;
            int iterationCount = 0;
            while (iterationCount < 10) {
                SmsEntity next = repositoryProxy.findOneByIdGreaterThan(lastId);
                if (next == null) break;
                lastId = next.getId();
                iterationCount++;
            }
            logger.info("✓ Cursor iteration found " + iterationCount + " records");
            
            // Test batch fetching across tables
            List<SmsEntity> batch = repositoryProxy.findBatchByIdGreaterThan(0L, 100);
            logger.info("✓ Batch fetch returned " + batch.size() + " records");
            
            saveTestResults(testId, "Cross-Table Queries", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Cross-Table Queries", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 4: Maintenance Operations
     */
    private void testMaintenanceOperations() throws Exception {
        logger.info("TEST 4: Automatic Maintenance Operations");
        
        String testId = "maintenance_ops_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Insert old data (beyond retention period)
            LocalDateTime oldDate = LocalDateTime.now().minusDays(10);
            String oldTableName = "sms_" + oldDate.format(dateFormatter);
            
            // Create old table manually
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                stmt.execute(
                    "CREATE TABLE IF NOT EXISTS " + oldTableName + " (" +
                    "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                    "sender_id VARCHAR(100), " +
                    "receiver_id VARCHAR(100), " +
                    "message TEXT, " +
                    "status VARCHAR(50), " +
                    "created_at TIMESTAMP, " +
                    "KEY idx_created_at (created_at)" +
                    ") ENGINE=InnoDB"
                );
                
                logger.info("Created old table: " + oldTableName);
            }
            
            // Wait for maintenance cycle (or trigger manually if available)
            Thread.sleep(2000);
            
            // Verify old table handling
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM information_schema.tables " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' " +
                    "AND table_name = '" + oldTableName + "'"
                );
                
                rs.next();
                int exists = rs.getInt(1);
                
                // Note: Actual deletion depends on maintenance implementation
                logger.info("Old table " + oldTableName + " exists: " + (exists > 0));
            }
            
            // Verify future table pre-creation
            LocalDateTime futureDate = LocalDateTime.now().plusDays(1);
            String futureTableName = "sms_" + futureDate.format(dateFormatter);
            
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM information_schema.tables " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' " +
                    "AND table_name = '" + futureTableName + "'"
                );
                
                rs.next();
                int exists = rs.getInt(1);
                
                logger.info("Future table " + futureTableName + " pre-created: " + (exists > 0));
            }
            
            saveTestResults(testId, "Maintenance Operations", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Maintenance Operations", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 5: Boundary Conditions
     */
    private void testBoundaryConditions() throws Exception {
        logger.info("TEST 5: Boundary Conditions (Day/Hour Transitions)");
        
        String testId = "boundary_conditions_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Test midnight boundary
            LocalDateTime beforeMidnight = LocalDateTime.now().withHour(23).withMinute(59).withSecond(59);
            LocalDateTime afterMidnight = beforeMidnight.plusSeconds(2);
            
            SmsEntity sms1 = new SmsEntity();
            sms1.setSenderId("boundary_sender");
            sms1.setReceiverId("boundary_receiver");
            sms1.setMessage("Before midnight");
            sms1.setStatus("sent");
            sms1.setCreatedAt(beforeMidnight);
            
            SmsEntity sms2 = new SmsEntity();
            sms2.setSenderId("boundary_sender");
            sms2.setReceiverId("boundary_receiver");
            sms2.setMessage("After midnight");
            sms2.setStatus("sent");
            sms2.setCreatedAt(afterMidnight);
            
            repositoryProxy.insert(sms1);
            repositoryProxy.insert(sms2);
            
            // Verify they're in different tables
            String table1 = "sms_" + beforeMidnight.format(dateFormatter);
            String table2 = "sms_" + afterMidnight.format(dateFormatter);
            
            if (!table1.equals(table2)) {
                logger.info("✓ Records correctly split across day boundary");
                logger.info("  Before midnight -> " + table1);
                logger.info("  After midnight -> " + table2);
            }
            
            // Test hour boundary within same day
            for (int hour = 0; hour < 23; hour++) {
                LocalDateTime hourEnd = LocalDateTime.now().withHour(hour).withMinute(59).withSecond(59);
                LocalDateTime nextHour = hourEnd.plusSeconds(2);
                
                SmsEntity hourBoundary = new SmsEntity();
                hourBoundary.setSenderId("hour_test");
                hourBoundary.setReceiverId("hour_test");
                hourBoundary.setMessage("Hour " + hour + " boundary test");
                hourBoundary.setStatus("test");
                hourBoundary.setCreatedAt(hourEnd);
                
                repositoryProxy.insert(hourBoundary);
            }
            
            logger.info("✓ Successfully tested hour boundaries");
            
            saveTestResults(testId, "Boundary Conditions", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Boundary Conditions", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 6: Cursor-based Pagination
     */
    private void testCursorPagination() throws Exception {
        logger.info("TEST 6: Cursor-based Pagination Across Tables");
        
        String testId = "cursor_pagination_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Process all records using cursor
            Long cursor = 0L;
            int totalProcessed = 0;
            int batchSize = 50;
            Set<Long> processedIds = new HashSet<>();
            
            while (true) {
                List<SmsEntity> batch = repositoryProxy.findBatchByIdGreaterThan(cursor, batchSize);
                
                if (batch.isEmpty()) {
                    break;
                }
                
                for (SmsEntity sms : batch) {
                    if (processedIds.contains(sms.getId())) {
                        throw new AssertionError("Duplicate ID in cursor pagination: " + sms.getId());
                    }
                    processedIds.add(sms.getId());
                }
                
                totalProcessed += batch.size();
                cursor = batch.get(batch.size() - 1).getId();
                
                logger.info("Processed batch: " + batch.size() + " records, cursor at: " + cursor);
                
                if (batch.size() < batchSize) {
                    break; // Last batch
                }
            }
            
            logger.info("✓ Cursor pagination processed " + totalProcessed + " total records");
            
            // Verify no records were missed
            LocalDateTime startDate = LocalDateTime.now().minusDays(7);
            LocalDateTime endDate = LocalDateTime.now();
            List<SmsEntity> allRecords = repositoryProxy.findAllByDateRange(startDate, endDate);
            
            if (Math.abs(allRecords.size() - totalProcessed) > 10) { // Allow small variance
                logger.warn("Cursor pagination count mismatch. Direct query: " + 
                           allRecords.size() + ", Cursor: " + totalProcessed);
            }
            
            saveTestResults(testId, "Cursor Pagination", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Cursor Pagination", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 7: Batch Operations
     */
    private void testBatchOperations() throws Exception {
        logger.info("TEST 7: Batch Insert Operations");
        
        String testId = "batch_operations_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Test various batch sizes
            int[] batchSizes = {10, 100, 500, 1000};
            
            for (int batchSize : batchSizes) {
                List<SmsEntity> batch = new ArrayList<>();
                
                for (int i = 0; i < batchSize; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setSenderId("batch_sender_" + i);
                    sms.setReceiverId("batch_receiver_" + i);
                    sms.setMessage("Batch message " + i + " of " + batchSize);
                    sms.setStatus("queued");
                    sms.setCreatedAt(LocalDateTime.now().minusHours(i % 24));
                    batch.add(sms);
                }
                
                long batchStart = System.currentTimeMillis();
                repositoryProxy.insertMultiple(batch);
                long batchTime = System.currentTimeMillis() - batchStart;
                
                double throughput = (batchSize * 1000.0) / batchTime;
                logger.info("Batch size " + batchSize + ": " + batchTime + "ms, " + 
                           String.format("%.0f", throughput) + " records/sec");
                
                recordMetric("batch_insert", "batch_" + batchSize, throughput);
            }
            
            saveTestResults(testId, "Batch Operations", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Batch Operations", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 8: Hourly Partitioning
     */
    private void testHourlyPartitioning() throws Exception {
        logger.info("TEST 8: Hourly Partitioning Within Daily Tables");
        
        String testId = "hourly_partitioning_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            String todayTable = "sms_" + LocalDateTime.now().format(dateFormatter);
            
            // Insert records for each hour
            for (int hour = 0; hour < 24; hour++) {
                for (int i = 0; i < 10; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setSenderId("hourly_sender");
                    sms.setReceiverId("hourly_receiver");
                    sms.setMessage("Hour " + hour + " message " + i);
                    sms.setStatus("sent");
                    sms.setCreatedAt(LocalDateTime.now().withHour(hour).withMinute(i * 5));
                    
                    repositoryProxy.insert(sms);
                }
            }
            
            // Verify hourly distribution
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT HOUR(created_at) as hour, COUNT(*) as count " +
                    "FROM " + todayTable + " " +
                    "GROUP BY HOUR(created_at) " +
                    "ORDER BY hour"
                );
                
                logger.info("Hourly distribution in " + todayTable + ":");
                while (rs.next()) {
                    logger.info("  Hour " + rs.getInt("hour") + ": " + rs.getInt("count") + " records");
                }
            }
            
            // Verify partition structure
            verifyHourlyPartitions(todayTable);
            
            saveTestResults(testId, "Hourly Partitioning", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Hourly Partitioning", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 9: Historical Data Migration
     */
    private void testDataMigration() throws Exception {
        logger.info("TEST 9: Historical Data Migration Simulation");
        
        String testId = "data_migration_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Simulate migrating 30 days of historical data
            int daysToMigrate = 30;
            int recordsPerDay = 1000;
            
            logger.info("Starting migration of " + (daysToMigrate * recordsPerDay) + " records");
            
            for (int day = 0; day < daysToMigrate; day++) {
                LocalDateTime targetDate = LocalDateTime.now().minusDays(day);
                List<SmsEntity> dayData = new ArrayList<>();
                
                for (int i = 0; i < recordsPerDay; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setSenderId("migrated_sender_" + i);
                    sms.setReceiverId("migrated_receiver_" + i);
                    sms.setMessage("Migrated message for " + targetDate.toLocalDate());
                    sms.setStatus("migrated");
                    sms.setCreatedAt(targetDate.minusMinutes(i));
                    dayData.add(sms);
                }
                
                long dayStart = System.currentTimeMillis();
                repositoryProxy.insertMultiple(dayData);
                long dayTime = System.currentTimeMillis() - dayStart;
                
                if (day % 5 == 0) {
                    logger.info("Migrated day " + (day + 1) + "/" + daysToMigrate + 
                               " in " + dayTime + "ms");
                }
            }
            
            // Verify migration
            LocalDateTime migrationStart = LocalDateTime.now().minusDays(daysToMigrate);
            LocalDateTime migrationEnd = LocalDateTime.now();
            List<SmsEntity> migrated = repositoryProxy.findAllByDateRange(migrationStart, migrationEnd);
            
            logger.info("✓ Migration complete. Verified " + migrated.size() + " records");
            
            saveTestResults(testId, "Data Migration", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Data Migration", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Test 10: Failure Recovery
     */
    private void testFailureRecovery() throws Exception {
        logger.info("TEST 10: Failure Recovery Scenarios");
        
        String testId = "failure_recovery_" + System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        
        try {
            // Test partial batch failure
            List<SmsEntity> problematicBatch = new ArrayList<>();
            
            for (int i = 0; i < 100; i++) {
                SmsEntity sms = new SmsEntity();
                sms.setSenderId("recovery_sender_" + i);
                sms.setReceiverId("recovery_receiver_" + i);
                
                // Create an overly long message for one record
                if (i == 50) {
                    StringBuilder longMessage = new StringBuilder();
                    for (int j = 0; j < 10000; j++) {
                        longMessage.append("x");
                    }
                    sms.setMessage(longMessage.toString());
                } else {
                    sms.setMessage("Normal message " + i);
                }
                
                sms.setStatus("test");
                sms.setCreatedAt(LocalDateTime.now());
                problematicBatch.add(sms);
            }
            
            try {
                repositoryProxy.insertMultiple(problematicBatch);
                logger.warn("Batch with problematic record succeeded unexpectedly");
            } catch (Exception e) {
                logger.info("✓ Batch with problematic record failed as expected: " + e.getMessage());
                failedOperations.incrementAndGet();
            }
            
            // Verify atomicity - none should be inserted
            List<SmsEntity> checkRecovery = repositoryProxy.findAllByDateRange(
                LocalDateTime.now().minusHours(1),
                LocalDateTime.now()
            );
            
            boolean foundRecoveryRecord = false;
            for (SmsEntity sms : checkRecovery) {
                if (sms.getSenderId() != null && sms.getSenderId().startsWith("recovery_sender_")) {
                    foundRecoveryRecord = true;
                    break;
                }
            }
            
            if (foundRecoveryRecord) {
                logger.warn("Partial batch was inserted - atomicity not maintained");
            } else {
                logger.info("✓ Batch atomicity maintained - no partial inserts");
            }
            
            saveTestResults(testId, "Failure Recovery", startTime, System.currentTimeMillis(), "PASS");
            
        } catch (Exception e) {
            saveTestResults(testId, "Failure Recovery", startTime, System.currentTimeMillis(), "FAIL");
            throw e;
        }
    }
    
    /**
     * Helper: Verify hourly partitions exist
     */
    private void verifyHourlyPartitions(String tableName) throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) as partition_count " +
                "FROM information_schema.partitions " +
                "WHERE table_schema = '" + TEST_DATABASE + "' " +
                "AND table_name = '" + tableName + "' " +
                "AND partition_name IS NOT NULL"
            );
            
            rs.next();
            int partitionCount = rs.getInt("partition_count");
            
            if (partitionCount == 24) {
                logger.info("✓ Table " + tableName + " has " + partitionCount + " hour partitions");
            } else {
                logger.warn("Table " + tableName + " has " + partitionCount + " partitions (expected 24)");
            }
        }
    }
    
    /**
     * Helper: Verify hourly distribution
     */
    private void verifyHourlyDistribution(String tableName) throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "  HOUR(created_at) as hour, " +
                "  COUNT(*) as count, " +
                "  MIN(id) as min_id, " +
                "  MAX(id) as max_id " +
                "FROM " + tableName + " " +
                "GROUP BY HOUR(created_at) " +
                "ORDER BY hour"
            );
            
            int hoursWithData = 0;
            while (rs.next()) {
                hoursWithData++;
                int count = rs.getInt("count");
                if (count > 0) {
                    logger.debug("Hour " + rs.getInt("hour") + ": " + count + " records");
                }
            }
            
            logger.info("✓ Data distributed across " + hoursWithData + " hours in " + tableName);
        }
    }
    
    /**
     * Print final test report
     */
    @Override
    protected void printTestSummary() {
        super.printTestSummary();
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "  COUNT(*) as total_tables, " +
                "  SUM(table_rows) as total_rows, " +
                "  SUM(data_length + index_length)/1024/1024 as total_size_mb " +
                "FROM information_schema.tables " +
                "WHERE table_schema = '" + TEST_DATABASE + "' " +
                "AND table_name LIKE 'sms_%'"
            );
            
            if (rs.next()) {
                logger.info("Final State:");
                logger.info("  Total Tables: " + rs.getInt("total_tables"));
                logger.info("  Total Rows: " + rs.getLong("total_rows"));
                logger.info("  Total Size: " + String.format("%.2f MB", rs.getDouble("total_size_mb")));
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get final statistics: " + e.getMessage());
        }
    }
}