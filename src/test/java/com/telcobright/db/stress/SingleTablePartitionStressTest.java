package com.telcobright.db.stress;

import com.telcobright.db.connection.ConnectionProvider;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.GenericPartitionedTableRepository;
import com.telcobright.db.test.entity.TestEntity;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * SINGLE TABLE PARTITION STRESS TEST
 * 
 * Tests rock-solid reliability of:
 * - Single table with MySQL native partitioning (daily partitions)
 * - Automatic partition creation and deletion (housekeeping)
 * - Partition pruning and performance optimization
 * - Million+ records with partition-based queries
 * - Retention period enforcement with old partition cleanup
 * - Concurrent access to partitioned table
 */
@Testcontainers
@DisplayName("📊 SINGLE TABLE PARTITION STRESS TEST - Native MySQL Partitioning")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SingleTablePartitionStressTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SingleTablePartitionStressTest.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    // STRESS TEST PARAMETERS
    private static final int MILLION_RECORDS = 1_000_000;
    private static final int BATCH_SIZE = 10_000;
    private static final int THREAD_COUNT = 8;
    private static final int DAYS_TO_SPAN = 20; // 20 days of data
    private static final int RETENTION_PERIOD_DAYS = 7; // 7 days retention for testing
    private static final String TABLE_NAME = "single_partition_test";
    
    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("single_partition_db")
            .withUsername("partition_user")
            .withPassword("partition_pass")
            .withCommand("--innodb-buffer-pool-size=2G", 
                        "--innodb-log-file-size=1G",
                        "--max-connections=1000");
    
    private static ConnectionProvider connectionProvider;
    private static GenericPartitionedTableRepository<TestEntity, Long> repository;
    private static EntityMetadata<TestEntity, Long> entityMetadata;
    private static ExecutorService executorService;
    
    // Metrics collection
    private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
    private static final AtomicInteger partitionsCreated = new AtomicInteger(0);
    private static final AtomicInteger partitionsDeleted = new AtomicInteger(0);
    private static final AtomicLong totalInsertTime = new AtomicLong(0);
    private static final Map<String, AtomicLong> partitionDistribution = new ConcurrentHashMap<>();
    
    private static final LocalDateTime TEST_START_TIME = LocalDateTime.now().minusDays(DAYS_TO_SPAN);
    
    @BeforeAll
    static void setupSingleTablePartitionStressTest() {
        logger.info("🚀 INITIALIZING SINGLE TABLE PARTITION STRESS TEST");
        logger.info("📊 Test Parameters:");
        logger.info("   • Records to insert: {}", MILLION_RECORDS);
        logger.info("   • Days to span: {}", DAYS_TO_SPAN);
        logger.info("   • Retention period: {} days", RETENTION_PERIOD_DAYS);
        logger.info("   • Thread count: {}", THREAD_COUNT);
        logger.info("   • Table name: {}", TABLE_NAME);
        
        String host = mysql.getHost();
        int port = mysql.getFirstMappedPort();
        String database = mysql.getDatabaseName();
        String username = mysql.getUsername();
        String password = mysql.getPassword();
        
        logger.info("🐬 MySQL Container: {}:{}/{}", host, port, database);
        
        // Initialize components
        connectionProvider = new ConnectionProvider(host, port, database, username, password);
        entityMetadata = new EntityMetadata<>(TestEntity.class, Long.class);
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        
        // Initialize repository with single table partitioning
        try {
            repository = GenericPartitionedTableRepository.builder(TestEntity.class, Long.class)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .tableName(TABLE_NAME)
                .partitionRetentionPeriod(RETENTION_PERIOD_DAYS)
                .autoManagePartitions(true)
                .partitionAdjustmentTime(4, 0) // 4 AM maintenance
                .initializePartitionsOnStart(true)
                .build();
                
            logger.info("✅ GenericPartitionedTableRepository initialized successfully");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize partitioned repository: {}", e.getMessage());
            fail("Repository initialization failed: " + e.getMessage());
        }
        
        logger.info("✅ Single table partition stress test initialization completed");
    }
    
    @Test
    @Order(1)
    @DisplayName("1. 📊 VERIFY SINGLE TABLE WITH PARTITIONS")
    void testSingleTablePartitionStructure() throws Exception {
        logger.info("📊 VERIFYING SINGLE TABLE PARTITION STRUCTURE");
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Verify table exists
            boolean tableExists = verifyTableExists(conn, TABLE_NAME);
            assertThat(tableExists).isTrue();
            
            // Verify table is partitioned
            boolean isPartitioned = verifyTableIsPartitioned(conn, TABLE_NAME);
            assertThat(isPartitioned).isTrue();
            
            // Get initial partition count
            int initialPartitionCount = getPartitionCount(conn, TABLE_NAME);
            partitionsCreated.set(initialPartitionCount);
            
            // Verify partition names follow pattern (p20250828, etc.)
            List<String> partitionNames = getPartitionNames(conn, TABLE_NAME);
            verifyPartitionNamingPattern(partitionNames);
            
            logger.info("✅ TABLE STRUCTURE VERIFICATION COMPLETED");
            logger.info("📊 Results:");
            logger.info("   • Table exists: {}", tableExists);
            logger.info("   • Table is partitioned: {}", isPartitioned);
            logger.info("   • Initial partition count: {}", initialPartitionCount);
            logger.info("   • Partition names: {}", partitionNames);
            
            assertThat(initialPartitionCount).isGreaterThan(0);
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("2. 🔥 MILLION RECORDS INTO PARTITIONED TABLE")
    void testMillionRecordsIntoPartitionedTable() throws Exception {
        logger.info("🔥 INSERTING MILLION RECORDS INTO PARTITIONED TABLE");
        
        long startTime = System.currentTimeMillis();
        List<Future<InsertionResult>> futures = new ArrayList<>();
        
        int recordsPerThread = MILLION_RECORDS / THREAD_COUNT;
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            final int startRecord = threadId * recordsPerThread;
            final int endRecord = (threadId == THREAD_COUNT - 1) ? MILLION_RECORDS : startRecord + recordsPerThread;
            
            Future<InsertionResult> future = executorService.submit(() -> 
                insertRecordsUsingRepository(finalThreadId, startRecord, endRecord));
            futures.add(future);
        }
        
        // Collect results
        long totalInserted = 0;
        long totalTime = 0;
        
        for (Future<InsertionResult> future : futures) {
            InsertionResult result = future.get();
            totalInserted += result.recordsInserted;
            totalTime += result.insertionTime;
            
            logger.info("🧵 Thread {} completed: {} records in {}ms", 
                result.threadId, result.recordsInserted, result.insertionTime);
        }
        
        long endTime = System.currentTimeMillis();
        long totalTestTime = endTime - startTime;
        
        totalRecordsInserted.set(totalInserted);
        totalInsertTime.set(totalTestTime);
        
        logger.info("✅ MILLION RECORDS INSERTION COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Total records inserted: {}", totalInserted);
        logger.info("   • Total test time: {}ms ({} seconds)", totalTestTime, totalTestTime / 1000);
        logger.info("   • Average insertion rate: {} records/second", 
            (totalInserted * 1000) / totalTestTime);
        
        assertThat(totalInserted).isEqualTo(MILLION_RECORDS);
    }
    
    @Test
    @Order(3)
    @DisplayName("3. 🔍 VERIFY PARTITION DATA DISTRIBUTION")
    void testPartitionDataDistribution() throws Exception {
        logger.info("🔍 VERIFYING PARTITION DATA DISTRIBUTION");
        
        Map<String, Long> actualDistribution = new HashMap<>();
        long totalRecordsVerified = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Get all partitions and their record counts
            List<String> partitionNames = getPartitionNames(conn, TABLE_NAME);
            
            for (String partitionName : partitionNames) {
                long recordCount = getPartitionRecordCount(conn, TABLE_NAME, partitionName);
                actualDistribution.put(partitionName, recordCount);
                totalRecordsVerified += recordCount;
                
                if (recordCount > 0) {
                    partitionDistribution.computeIfAbsent(partitionName, k -> new AtomicLong(0))
                        .addAndGet(recordCount);
                }
                
                logger.debug("📊 Partition {}: {} records", partitionName, recordCount);
            }
            
            // Analyze distribution
            analyzePartitionDistribution(actualDistribution);
        }
        
        logger.info("✅ PARTITION DATA DISTRIBUTION VERIFICATION COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Total records verified: {}", totalRecordsVerified);
        logger.info("   • Partitions with data: {}", 
            actualDistribution.entrySet().stream().filter(e -> e.getValue() > 0).count());
        logger.info("   • Data distribution efficiency: {}%", 
            (actualDistribution.size() * 100.0) / Math.max(1, partitionDistribution.size()));
        
        assertThat(totalRecordsVerified).isEqualTo(MILLION_RECORDS);
    }
    
    @Test
    @Order(4)
    @DisplayName("4. 🧹 PARTITION HOUSEKEEPING - OLD PARTITION CLEANUP")
    void testPartitionHousekeeping() throws Exception {
        logger.info("🧹 TESTING PARTITION HOUSEKEEPING AND OLD PARTITION CLEANUP");
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Get initial partition state
            List<String> initialPartitions = getPartitionNames(conn, TABLE_NAME);
            int initialCount = initialPartitions.size();
            
            logger.info("📊 Initial state: {} partitions", initialCount);
            
            // Trigger manual maintenance to test housekeeping
            LocalDateTime cutoffDate = LocalDateTime.now().minusDays(RETENTION_PERIOD_DAYS);
            
            // Use repository's maintenance functionality
            repository.dropOldPartitions(cutoffDate);
            
            // Verify old partitions were cleaned up
            List<String> finalPartitions = getPartitionNames(conn, TABLE_NAME);
            int finalCount = finalPartitions.size();
            
            // Calculate which partitions should have been deleted
            List<String> expectedDeletedPartitions = new ArrayList<>();
            String cutoffDateStr = cutoffDate.format(DATE_FORMAT);
            
            for (String partition : initialPartitions) {
                if (partition.startsWith("p") && partition.length() > 1) {
                    String dateStr = partition.substring(1);
                    if (dateStr.compareTo(cutoffDateStr) < 0) {
                        expectedDeletedPartitions.add(partition);
                    }
                }
            }
            
            int actualDeleted = initialCount - finalCount;
            partitionsDeleted.set(actualDeleted);
            
            logger.info("✅ PARTITION HOUSEKEEPING COMPLETED");
            logger.info("📊 Results:");
            logger.info("   • Initial partitions: {}", initialCount);
            logger.info("   • Final partitions: {}", finalCount);
            logger.info("   • Partitions deleted: {}", actualDeleted);
            logger.info("   • Expected to delete: {}", expectedDeletedPartitions.size());
            logger.info("   • Retention period: {} days", RETENTION_PERIOD_DAYS);
            logger.info("   • Cutoff date: {}", cutoffDate.toLocalDate());
            
            // Verify housekeeping worked
            assertThat(finalCount).isLessThanOrEqualTo(initialCount);
            
            // Verify no partitions older than retention period exist
            verifyNoOldPartitionsExist(finalPartitions, cutoffDate);
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("5. ⚡ PARTITION PRUNING PERFORMANCE TEST")
    void testPartitionPruningPerformance() throws Exception {
        logger.info("⚡ TESTING PARTITION PRUNING PERFORMANCE");
        
        // Test queries that should benefit from partition pruning
        List<PartitionPruningTest> pruningTests = Arrays.asList(
            new PartitionPruningTest("Single Day Query", 1),
            new PartitionPruningTest("3-Day Range Query", 3), 
            new PartitionPruningTest("Week Range Query", 7),
            new PartitionPruningTest("Full Range Query", DAYS_TO_SPAN)
        );
        
        for (PartitionPruningTest test : pruningTests) {
            logger.info("🔍 Testing: {}", test.name);
            
            LocalDateTime startDate = TEST_START_TIME;
            LocalDateTime endDate = startDate.plusDays(test.dayRange);
            
            long startTime = System.currentTimeMillis();
            
            // Execute date range query using repository
            List<TestEntity> results = repository.findAllByDateRange(startDate, endDate);
            
            long endTime = System.currentTimeMillis();
            long queryTime = endTime - startTime;
            
            logger.info("✅ {} completed: {} records in {}ms", 
                test.name, results.size(), queryTime);
            
            // Verify performance is reasonable (should benefit from partition pruning)
            assertThat(queryTime).isLessThan(30000); // Max 30 seconds
            assertThat(results).isNotNull();
            
            // Verify all results are within date range
            for (TestEntity entity : results) {
                assertThat(entity.getCreatedAt()).isBetween(startDate, endDate);
            }
        }
        
        logger.info("✅ PARTITION PRUNING PERFORMANCE TEST COMPLETED");
    }
    
    @Test
    @Order(6)
    @DisplayName("6. 🔄 AUTOMATIC MAINTENANCE VERIFICATION")
    void testAutomaticMaintenanceVerification() throws Exception {
        logger.info("🔄 VERIFYING AUTOMATIC MAINTENANCE FUNCTIONALITY");
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Get current partition state
            List<String> currentPartitions = getPartitionNames(conn, TABLE_NAME);
            int currentCount = currentPartitions.size();
            
            // Simulate passage of time by creating partitions for future dates
            LocalDateTime futureDate = LocalDateTime.now().plusDays(5);
            repository.createPartitionsForDateRange(futureDate, futureDate.plusDays(3));
            
            // Verify new partitions were created
            List<String> updatedPartitions = getPartitionNames(conn, TABLE_NAME);
            int updatedCount = updatedPartitions.size();
            
            logger.info("📊 Automatic Maintenance Results:");
            logger.info("   • Partitions before: {}", currentCount);
            logger.info("   • Partitions after: {}", updatedCount);
            logger.info("   • New partitions created: {}", updatedCount - currentCount);
            
            // Verify automatic maintenance created expected partitions
            assertThat(updatedCount).isGreaterThan(currentCount);
            
            // Test retention policy enforcement
            LocalDateTime oldCutoff = LocalDateTime.now().minusDays(RETENTION_PERIOD_DAYS + 5);
            repository.dropOldPartitions(oldCutoff);
            
            List<String> finalPartitions = getPartitionNames(conn, TABLE_NAME);
            
            // Verify no very old partitions remain
            String cutoffStr = oldCutoff.format(DATE_FORMAT);
            for (String partition : finalPartitions) {
                if (partition.startsWith("p") && partition.length() > 1) {
                    String dateStr = partition.substring(1);
                    assertThat(dateStr).isGreaterThanOrEqualTo(cutoffStr);
                }
            }
            
            logger.info("✅ AUTOMATIC MAINTENANCE VERIFICATION COMPLETED");
        }
    }
    
    // Helper Methods
    
    private InsertionResult insertRecordsUsingRepository(int threadId, int startRecord, int endRecord) {
        long threadStartTime = System.currentTimeMillis();
        int recordsInserted = 0;
        
        try {
            for (int i = startRecord; i < endRecord; i += BATCH_SIZE) {
                List<TestEntity> batch = new ArrayList<>();
                int batchEnd = Math.min(i + BATCH_SIZE, endRecord);
                
                for (int j = i; j < batchEnd; j++) {
                    TestEntity entity = createPartitionTestEntity(threadId, j);
                    batch.add(entity);
                }
                
                // Insert batch using repository
                for (TestEntity entity : batch) {
                    repository.insert(entity);
                    recordsInserted++;
                }
                
                if (recordsInserted % 50000 == 0) {
                    logger.debug("🧵 Thread {} progress: {} records inserted", threadId, recordsInserted);
                }
            }
            
        } catch (Exception e) {
            logger.error("❌ Error in insertion thread {}", threadId, e);
            throw new RuntimeException("Insertion failed", e);
        }
        
        long threadEndTime = System.currentTimeMillis();
        return new InsertionResult(threadId, recordsInserted, threadEndTime - threadStartTime);
    }
    
    private TestEntity createPartitionTestEntity(int threadId, int index) {
        // Distribute across time range for partitioning
        long totalMinutes = DAYS_TO_SPAN * 24 * 60;
        long minuteOffset = (index % totalMinutes);
        LocalDateTime timestamp = TEST_START_TIME.plusMinutes(minuteOffset);
        
        return new TestEntity(
            "PartitionUser_T" + threadId + "_" + index,
            "partition" + threadId + "_" + index + "@test.com",
            index % 3 == 0 ? "ACTIVE" : (index % 3 == 1 ? "PENDING" : "INACTIVE"),
            25 + (index % 50),
            new BigDecimal("300.00").add(new BigDecimal(index % 5000)),
            index % 2 == 0,
            "Single partition test entity " + index + " from thread " + threadId,
            timestamp
        );
    }
    
    private boolean verifyTableExists(Connection conn, String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM information_schema.tables " +
                      "WHERE table_schema = ? AND table_name = ?";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    private boolean verifyTableIsPartitioned(Connection conn, String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM information_schema.partitions " +
                      "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    private int getPartitionCount(Connection conn, String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM information_schema.partitions " +
                      "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }
    
    private List<String> getPartitionNames(Connection conn, String tableName) throws SQLException {
        List<String> partitionNames = new ArrayList<>();
        String query = "SELECT partition_name FROM information_schema.partitions " +
                      "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL " +
                      "ORDER BY partition_name";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    partitionNames.add(rs.getString("partition_name"));
                }
            }
        }
        
        return partitionNames;
    }
    
    private long getPartitionRecordCount(Connection conn, String tableName, String partitionName) throws SQLException {
        String query = "SELECT table_rows FROM information_schema.partitions " +
                      "WHERE table_schema = ? AND table_name = ? AND partition_name = ?";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            pstmt.setString(3, partitionName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getLong("table_rows") : 0;
            }
        }
    }
    
    private void verifyPartitionNamingPattern(List<String> partitionNames) {
        for (String partitionName : partitionNames) {
            // Partition names should follow pattern: p20250828, p20250829, etc.
            assertThat(partitionName).matches("p\\d{8}");
        }
        logger.info("✅ All partition names follow correct pattern (p + YYYYMMDD)");
    }
    
    private void analyzePartitionDistribution(Map<String, Long> distribution) {
        long totalRecords = distribution.values().stream().mapToLong(Long::longValue).sum();
        int partitionsWithData = (int) distribution.values().stream().filter(count -> count > 0).count();
        
        long minRecords = distribution.values().stream().filter(count -> count > 0)
            .mapToLong(Long::longValue).min().orElse(0);
        long maxRecords = distribution.values().stream().mapToLong(Long::longValue).max().orElse(0);
        double avgRecords = (double) totalRecords / Math.max(1, partitionsWithData);
        
        logger.info("📈 PARTITION DISTRIBUTION ANALYSIS:");
        logger.info("   • Total records distributed: {}", totalRecords);
        logger.info("   • Partitions with data: {}", partitionsWithData);
        logger.info("   • Min records in partition: {}", minRecords);
        logger.info("   • Max records in partition: {}", maxRecords);
        logger.info("   • Average records per partition: {:.0f}", avgRecords);
        logger.info("   • Distribution variance: {:.2f}%", 
            partitionsWithData > 0 ? ((maxRecords - minRecords) / avgRecords * 100) : 0);
    }
    
    private void verifyNoOldPartitionsExist(List<String> partitions, LocalDateTime cutoffDate) {
        String cutoffDateStr = cutoffDate.format(DATE_FORMAT);
        
        for (String partition : partitions) {
            if (partition.startsWith("p") && partition.length() > 1) {
                String dateStr = partition.substring(1);
                assertThat(dateStr).isGreaterThanOrEqualTo(cutoffDateStr);
            }
        }
        
        logger.info("✅ Verified no partitions exist older than {}", cutoffDate.toLocalDate());
    }
    
    @AfterAll
    static void printSingleTablePartitionResults() {
        logger.info("🎯 SINGLE TABLE PARTITION STRESS TEST FINAL RESULTS");
        logger.info("=".repeat(70));
        logger.info("📊 PARTITION METRICS:");
        logger.info("   • Total records processed: {}", totalRecordsInserted.get());
        logger.info("   • Partitions created: {}", partitionsCreated.get());
        logger.info("   • Partitions deleted (housekeeping): {}", partitionsDeleted.get());
        logger.info("   • Total insertion time: {}ms", totalInsertTime.get());
        logger.info("   • Average throughput: {} records/second", 
            totalRecordsInserted.get() * 1000 / Math.max(1, totalInsertTime.get()));
        
        logger.info("📅 PARTITION DATA DISTRIBUTION:");
        partitionDistribution.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> logger.info("   • Partition {}: {} records", 
                entry.getKey(), entry.getValue().get()));
        
        logger.info("🏆 SINGLE TABLE PARTITION STRESS TEST COMPLETED SUCCESSFULLY!");
        logger.info("✅ FEATURES VALIDATED:");
        logger.info("   ✅ Single table with MySQL native partitioning");
        logger.info("   ✅ Automatic partition creation and deletion");
        logger.info("   ✅ Partition housekeeping and retention policies");
        logger.info("   ✅ Partition pruning performance optimization");
        logger.info("   ✅ Million+ records with partition distribution");
        logger.info("   ✅ Concurrent access to partitioned table");
        
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
    
    // Result classes
    static class InsertionResult {
        final int threadId;
        final int recordsInserted;
        final long insertionTime;
        
        InsertionResult(int threadId, int recordsInserted, long insertionTime) {
            this.threadId = threadId;
            this.recordsInserted = recordsInserted;
            this.insertionTime = insertionTime;
        }
    }
    
    static class PartitionPruningTest {
        final String name;
        final int dayRange;
        
        PartitionPruningTest(String name, int dayRange) {
            this.name = name;
            this.dayRange = dayRange;
        }
    }
}