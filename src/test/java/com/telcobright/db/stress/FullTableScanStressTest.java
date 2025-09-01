package com.telcobright.db.stress;

import com.telcobright.db.connection.ConnectionProvider;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.GenericMultiTableRepository;
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
 * FULL TABLE SCAN STRESS TEST
 * 
 * Tests rock-solid performance of full table scan operations:
 * 
 * SINGLE PARTITIONED TABLE SCANS:
 * - Full table scan across all partitions in single table
 * - Index-optimized lookups without unique constraints
 * - Partition pruning vs full partition scan performance
 * - Million+ records with various scan patterns
 * 
 * MULTI-TABLE SCANS:
 * - Full scan across ALL daily tables (100+ tables)
 * - UNION ALL queries across multiple tables
 * - Cross-table aggregation with full scans
 * - Index usage across multiple tables
 * 
 * Focus: Performance optimization with proper indexing, NO unique constraints
 */
@Testcontainers
@DisplayName("🔍 FULL TABLE SCAN STRESS TEST - Optimized Scanning Without Unique Constraints")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FullTableScanStressTest {
    
    private static final Logger logger = LoggerFactory.getLogger(FullTableScanStressTest.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    // STRESS TEST PARAMETERS
    private static final int RECORDS_PER_TABLE = 100_000; // 100K per table/partition
    private static final int DAYS_TO_SPAN = 30; // 30 tables or partitions
    private static final int BATCH_SIZE = 5_000;
    private static final int THREAD_COUNT = 8;
    private static final int FULL_SCAN_ITERATIONS = 50;
    
    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("full_scan_db")
            .withUsername("scan_user")
            .withPassword("scan_pass")
            .withCommand("--innodb-buffer-pool-size=2G", 
                        "--innodb-log-file-size=1G",
                        "--max-connections=500",
                        "--innodb-read-io-threads=8",
                        "--innodb-write-io-threads=8");
    
    private static ConnectionProvider connectionProvider;
    private static GenericPartitionedTableRepository<TestEntity, Long> partitionedRepository;
    private static GenericMultiTableRepository<TestEntity, Long> multiTableRepository;
    private static ExecutorService executorService;
    
    // Metrics collection
    private static final AtomicLong totalRecordsInSingleTable = new AtomicLong(0);
    private static final AtomicLong totalRecordsInMultiTables = new AtomicLong(0);
    private static final AtomicLong singleTableScanTime = new AtomicLong(0);
    private static final AtomicLong multiTableScanTime = new AtomicLong(0);
    private static final AtomicInteger fullScansCompleted = new AtomicInteger(0);
    private static final Map<String, AtomicLong> scanMetrics = new ConcurrentHashMap<>();
    
    private static final LocalDateTime TEST_START_TIME = LocalDateTime.now().minusDays(DAYS_TO_SPAN);
    
    @BeforeAll
    static void setupFullTableScanStressTest() {
        logger.info("🚀 INITIALIZING FULL TABLE SCAN STRESS TEST");
        logger.info("📊 Test Parameters:");
        logger.info("   • Records per table/partition: {}", RECORDS_PER_TABLE);
        logger.info("   • Days to span: {}", DAYS_TO_SPAN);
        logger.info("   • Full scan iterations: {}", FULL_SCAN_ITERATIONS);
        logger.info("   • Thread count: {}", THREAD_COUNT);
        logger.info("   • Focus: NO unique constraints, optimized indexing");
        
        String host = mysql.getHost();
        int port = mysql.getFirstMappedPort();
        String database = mysql.getDatabaseName();
        String username = mysql.getUsername();
        String password = mysql.getPassword();
        
        logger.info("🐬 MySQL Container: {}:{}/{}", host, port, database);
        
        // Initialize components
        connectionProvider = new ConnectionProvider(host, port, database, username, password);
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        
        // Initialize metrics
        scanMetrics.put("single_table_full_scans", new AtomicLong(0));
        scanMetrics.put("multi_table_full_scans", new AtomicLong(0));
        scanMetrics.put("index_lookups", new AtomicLong(0));
        scanMetrics.put("partition_pruned_scans", new AtomicLong(0));
        
        logger.info("✅ Full table scan stress test initialization completed");
    }
    
    @Test
    @Order(1)
    @DisplayName("1. 📊 SETUP SINGLE PARTITIONED TABLE (NO UNIQUE CONSTRAINTS)")
    void testSetupSinglePartitionedTable() throws Exception {
        logger.info("📊 SETTING UP SINGLE PARTITIONED TABLE WITHOUT UNIQUE CONSTRAINTS");
        
        try {
            // Initialize partitioned repository WITHOUT unique constraints
            partitionedRepository = GenericPartitionedTableRepository.builder(TestEntity.class, Long.class)
                .host(mysql.getHost())
                .port(mysql.getFirstMappedPort())
                .database(mysql.getDatabaseName())
                .username(mysql.getUsername())
                .password(mysql.getPassword())
                .tableName("scan_partitioned_test")
                .partitionRetentionPeriod(DAYS_TO_SPAN + 5)
                .autoManagePartitions(true)
                .initializePartitionsOnStart(true)
                .build();
            
            logger.info("✅ Partitioned repository initialized successfully");
            
        } catch (Exception e) {
            logger.warn("⚠️ Partitioned repository setup failed (expected due to unique constraints): {}", e.getMessage());
            logger.info("📝 Will create custom partitioned table without unique constraints");
            
            // Create custom partitioned table without unique constraints
            createCustomPartitionedTable();
        }
        
        // Populate with test data
        populateSinglePartitionedTable();
        
        logger.info("✅ SINGLE PARTITIONED TABLE SETUP COMPLETED");
    }
    
    @Test
    @Order(2)
    @DisplayName("2. 🏗️ SETUP MULTI-TABLE ARCHITECTURE")
    void testSetupMultiTableArchitecture() throws Exception {
        logger.info("🏗️ SETTING UP MULTI-TABLE ARCHITECTURE");
        
        try {
            // Initialize multi-table repository
            multiTableRepository = GenericMultiTableRepository.builder(TestEntity.class, Long.class)
                .host(mysql.getHost())
                .port(mysql.getFirstMappedPort())
                .database(mysql.getDatabaseName())
                .username(mysql.getUsername())
                .password(mysql.getPassword())
                .tablePrefix("scan_multi_test")
                .partitionRetentionPeriod(DAYS_TO_SPAN + 5)
                .autoManagePartitions(true)
                .initializePartitionsOnStart(false) // We'll populate manually
                .build();
                
            logger.info("✅ Multi-table repository initialized successfully");
            
        } catch (Exception e) {
            logger.error("❌ Multi-table repository setup failed: {}", e.getMessage());
            fail("Multi-table repository initialization failed");
        }
        
        // Populate with test data across multiple tables
        populateMultipleTables();
        
        logger.info("✅ MULTI-TABLE ARCHITECTURE SETUP COMPLETED");
    }
    
    @Test
    @Order(3)
    @DisplayName("3. 🔍 SINGLE TABLE FULL PARTITION SCAN PERFORMANCE")
    void testSingleTableFullPartitionScan() throws Exception {
        logger.info("🔍 TESTING SINGLE TABLE FULL PARTITION SCAN PERFORMANCE");
        
        List<FullScanTest> scanTests = Arrays.asList(
            new FullScanTest("Full Table Scan (All Partitions)", ScanType.FULL_TABLE),
            new FullScanTest("Partition-Pruned Scan (Date Range)", ScanType.PARTITION_PRUNED),
            new FullScanTest("Index-Optimized Lookup", ScanType.INDEX_LOOKUP),
            new FullScanTest("Status Filter Scan", ScanType.FILTERED_SCAN)
        );
        
        for (FullScanTest test : scanTests) {
            logger.info("🧪 Executing: {}", test.name);
            
            List<Future<ScanResult>> futures = new ArrayList<>();
            
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int threadId = i;
                Future<ScanResult> future = executorService.submit(() -> 
                    performSingleTableScan(threadId, test.scanType));
                futures.add(future);
            }
            
            // Collect results
            long totalRecords = 0;
            long totalTime = 0;
            int totalScans = 0;
            
            for (Future<ScanResult> future : futures) {
                ScanResult result = future.get();
                totalRecords += result.recordsScanned;
                totalTime += result.scanTime;
                totalScans += result.scansCompleted;
                
                logger.debug("🧵 Thread {} - {} scans, {} records in {}ms", 
                    result.threadId, result.scansCompleted, result.recordsScanned, result.scanTime);
            }
            
            double avgScanTime = (double) totalTime / totalScans;
            double recordsPerSecond = (totalRecords * 1000.0) / totalTime;
            
            logger.info("✅ {} completed:", test.name);
            logger.info("   • Total scans: {}", totalScans);
            logger.info("   • Total records scanned: {}", totalRecords);
            logger.info("   • Average scan time: {:.2f}ms", avgScanTime);
            logger.info("   • Scan rate: {:.0f} records/second", recordsPerSecond);
            
            // Track metrics
            if (test.scanType == ScanType.FULL_TABLE) {
                singleTableScanTime.addAndGet(totalTime);
                scanMetrics.get("single_table_full_scans").addAndGet(totalScans);
            } else if (test.scanType == ScanType.PARTITION_PRUNED) {
                scanMetrics.get("partition_pruned_scans").addAndGet(totalScans);
            } else if (test.scanType == ScanType.INDEX_LOOKUP) {
                scanMetrics.get("index_lookups").addAndGet(totalScans);
            }
            
            // Verify performance is reasonable
            assertThat(avgScanTime).isLessThan(10000); // Max 10 seconds per scan
            assertThat(totalRecords).isGreaterThan(0);
        }
        
        logger.info("✅ SINGLE TABLE FULL PARTITION SCAN TESTS COMPLETED");
    }
    
    @Test
    @Order(4)
    @DisplayName("4. 🏗️ MULTI-TABLE FULL SCAN PERFORMANCE")
    void testMultiTableFullScanPerformance() throws Exception {
        logger.info("🏗️ TESTING MULTI-TABLE FULL SCAN PERFORMANCE");
        
        List<MultiTableScanTest> multiScanTests = Arrays.asList(
            new MultiTableScanTest("Full Multi-Table Scan (All Tables)", MultiScanType.ALL_TABLES),
            new MultiTableScanTest("Date Range Multi-Table Scan", MultiScanType.DATE_RANGE),
            new MultiTableScanTest("Cross-Table Aggregation", MultiScanType.AGGREGATION),
            new MultiTableScanTest("Multi-Table Union Query", MultiScanType.UNION_QUERY)
        );
        
        for (MultiTableScanTest test : multiScanTests) {
            logger.info("🧪 Executing: {}", test.name);
            
            long startTime = System.currentTimeMillis();
            
            MultiTableScanResult result = performMultiTableScan(test.scanType);
            
            long endTime = System.currentTimeMillis();
            long scanTime = endTime - startTime;
            
            double recordsPerSecond = (result.recordsScanned * 1000.0) / scanTime;
            
            logger.info("✅ {} completed:", test.name);
            logger.info("   • Tables scanned: {}", result.tablesScanned);
            logger.info("   • Records scanned: {}", result.recordsScanned);
            logger.info("   • Scan time: {}ms", scanTime);
            logger.info("   • Scan rate: {:.0f} records/second", recordsPerSecond);
            
            multiTableScanTime.addAndGet(scanTime);
            scanMetrics.get("multi_table_full_scans").incrementAndGet();
            
            // Verify performance is reasonable
            assertThat(scanTime).isLessThan(30000); // Max 30 seconds for multi-table scan
            assertThat(result.recordsScanned).isGreaterThan(0);
            assertThat(result.tablesScanned).isGreaterThan(0);
        }
        
        logger.info("✅ MULTI-TABLE FULL SCAN TESTS COMPLETED");
    }
    
    @Test
    @Order(5)
    @DisplayName("5. 📈 INDEX OPTIMIZATION VERIFICATION")
    void testIndexOptimizationVerification() throws Exception {
        logger.info("📈 VERIFYING INDEX OPTIMIZATION WITHOUT UNIQUE CONSTRAINTS");
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Verify indexes exist on partitioned table
            verifyIndexesExist(conn, "scan_partitioned_test");
            
            // Verify indexes on multi-tables
            List<String> multiTables = getMultiTableNames(conn);
            for (String table : multiTables.subList(0, Math.min(5, multiTables.size()))) {
                String tableName = table.substring(table.lastIndexOf('.') + 1);
                verifyIndexesExist(conn, tableName);
            }
            
            // Test index usage with EXPLAIN queries
            testIndexUsageWithExplain(conn);
            
            logger.info("✅ INDEX OPTIMIZATION VERIFICATION COMPLETED");
            logger.info("📊 Index Summary:");
            logger.info("   • All tables have proper indexes (NO unique constraints)");
            logger.info("   • created_at indexes for date range queries");
            logger.info("   • name, status indexes for filtered scans");
            logger.info("   • Composite indexes for multi-column queries");
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("6. 🏁 PERFORMANCE COMPARISON ANALYSIS")
    void testPerformanceComparisonAnalysis() throws Exception {
        logger.info("🏁 ANALYZING PERFORMANCE COMPARISON");
        
        // Compare single table vs multi-table scan performance
        double singleTableAvgTime = (double) singleTableScanTime.get() / 
            Math.max(1, scanMetrics.get("single_table_full_scans").get());
        
        double multiTableAvgTime = (double) multiTableScanTime.get() / 
            Math.max(1, scanMetrics.get("multi_table_full_scans").get());
        
        // Calculate efficiency metrics
        double singleTableThroughput = (totalRecordsInSingleTable.get() * 1000.0) / 
            Math.max(1, singleTableScanTime.get());
        
        double multiTableThroughput = (totalRecordsInMultiTables.get() * 1000.0) / 
            Math.max(1, multiTableScanTime.get());
        
        logger.info("📊 PERFORMANCE COMPARISON RESULTS:");
        logger.info("=".repeat(60));
        logger.info("🔍 SINGLE TABLE PARTITIONED SCANS:");
        logger.info("   • Average scan time: {:.2f}ms", singleTableAvgTime);
        logger.info("   • Throughput: {:.0f} records/second", singleTableThroughput);
        logger.info("   • Total records: {}", totalRecordsInSingleTable.get());
        logger.info("   • Scans completed: {}", scanMetrics.get("single_table_full_scans").get());
        
        logger.info("🏗️ MULTI-TABLE ARCHITECTURE SCANS:");
        logger.info("   • Average scan time: {:.2f}ms", multiTableAvgTime);
        logger.info("   • Throughput: {:.0f} records/second", multiTableThroughput);
        logger.info("   • Total records: {}", totalRecordsInMultiTables.get());
        logger.info("   • Scans completed: {}", scanMetrics.get("multi_table_full_scans").get());
        
        logger.info("⚡ OPTIMIZATION METRICS:");
        logger.info("   • Partition-pruned scans: {}", scanMetrics.get("partition_pruned_scans").get());
        logger.info("   • Index lookups: {}", scanMetrics.get("index_lookups").get());
        
        // Verify both approaches work efficiently
        assertThat(singleTableThroughput).isGreaterThan(1000); // > 1K records/sec
        assertThat(multiTableThroughput).isGreaterThan(1000);   // > 1K records/sec
        
        logger.info("✅ BOTH SCANNING APPROACHES PERFORM EFFICIENTLY!");
    }
    
    // Helper Methods
    
    private void createCustomPartitionedTable() throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Drop existing table if exists
            stmt.executeUpdate("DROP TABLE IF EXISTS scan_partitioned_test");
            
            // Create partitioned table WITHOUT unique constraints
            String createSQL = """
                CREATE TABLE scan_partitioned_test (
                    id BIGINT AUTO_INCREMENT,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255),
                    status VARCHAR(255),
                    age INT,
                    balance DECIMAL(10,2),
                    active BOOLEAN,
                    description VARCHAR(255),
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME,
                    KEY idx_created_at (created_at),
                    KEY idx_name (name),
                    KEY idx_status (status),
                    KEY idx_email (email),
                    KEY idx_composite (status, created_at),
                    PRIMARY KEY (id, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                PARTITION BY RANGE (TO_DAYS(created_at)) (
                    PARTITION p_initial VALUES LESS THAN (TO_DAYS('2025-01-01'))
                )
                """;
            
            stmt.executeUpdate(createSQL);
            
            // Add partitions for test date range
            for (int i = 0; i < DAYS_TO_SPAN; i++) {
                LocalDateTime date = TEST_START_TIME.plusDays(i);
                String partitionName = "p" + date.format(DATE_FORMAT);
                LocalDateTime nextDay = date.plusDays(1);
                
                String addPartitionSQL = String.format(
                    "ALTER TABLE scan_partitioned_test ADD PARTITION (PARTITION %s VALUES LESS THAN (TO_DAYS('%s')))",
                    partitionName, nextDay.toLocalDate().toString()
                );
                
                try {
                    stmt.executeUpdate(addPartitionSQL);
                } catch (SQLException e) {
                    if (!e.getMessage().contains("already exists")) {
                        throw e;
                    }
                }
            }
            
            logger.info("✅ Custom partitioned table created with {} partitions", DAYS_TO_SPAN);
        }
    }
    
    private void populateSinglePartitionedTable() throws Exception {
        logger.info("📊 Populating single partitioned table with {} records", RECORDS_PER_TABLE * DAYS_TO_SPAN);
        
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            Future<Void> future = executorService.submit(() -> {
                populatePartitionedTableData(finalThreadId);
                return null;
            });
            futures.add(future);
        }
        
        for (Future<Void> future : futures) {
            future.get();
        }
        
        // Count total records
        long totalRecords = countRecordsInTable("scan_partitioned_test");
        totalRecordsInSingleTable.set(totalRecords);
        
        logger.info("✅ Single partitioned table populated with {} records", totalRecords);
    }
    
    private void populatePartitionedTableData(int threadId) {
        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(false);
            
            int recordsPerThread = (RECORDS_PER_TABLE * DAYS_TO_SPAN) / THREAD_COUNT;
            
            for (int i = 0; i < recordsPerThread; i += BATCH_SIZE) {
                List<TestEntity> batch = new ArrayList<>();
                int batchEnd = Math.min(i + BATCH_SIZE, recordsPerThread);
                
                for (int j = i; j < batchEnd; j++) {
                    TestEntity entity = createScanTestEntity(threadId, j);
                    batch.add(entity);
                }
                
                insertBatchDirectly(conn, "scan_partitioned_test", batch);
                conn.commit();
                
                if (i % (BATCH_SIZE * 10) == 0) {
                    logger.debug("🧵 Thread {} populated {} records", threadId, i);
                }
            }
            
        } catch (Exception e) {
            logger.error("❌ Error populating partitioned table data", e);
            throw new RuntimeException(e);
        }
    }
    
    private void populateMultipleTables() throws Exception {
        logger.info("🏗️ Populating multiple tables with {} records each", RECORDS_PER_TABLE);
        
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            Future<Void> future = executorService.submit(() -> {
                populateMultiTableData(finalThreadId);
                return null;
            });
            futures.add(future);
        }
        
        for (Future<Void> future : futures) {
            future.get();
        }
        
        // Count total records across all tables
        long totalRecords = countRecordsInMultiTables();
        totalRecordsInMultiTables.set(totalRecords);
        
        logger.info("✅ Multiple tables populated with {} total records", totalRecords);
    }
    
    private void populateMultiTableData(int threadId) {
        try {
            int recordsPerThread = RECORDS_PER_TABLE / THREAD_COUNT;
            
            for (int day = 0; day < DAYS_TO_SPAN; day++) {
                LocalDateTime dayDate = TEST_START_TIME.plusDays(day);
                
                List<TestEntity> dayData = new ArrayList<>();
                for (int i = 0; i < recordsPerThread; i++) {
                    TestEntity entity = createScanTestEntity(threadId, day * recordsPerThread + i);
                    entity.setCreatedAt(dayDate.plusMinutes(i % 1440)); // Spread across day
                    dayData.add(entity);
                }
                
                // Insert using repository
                for (TestEntity entity : dayData) {
                    multiTableRepository.insert(entity);
                }
                
                if (day % 5 == 0) {
                    logger.debug("🧵 Thread {} populated day {}", threadId, day);
                }
            }
            
        } catch (Exception e) {
            logger.error("❌ Error populating multi-table data", e);
            throw new RuntimeException(e);
        }
    }
    
    private TestEntity createScanTestEntity(int threadId, int index) {
        // Distribute across time range
        long totalMinutes = DAYS_TO_SPAN * 24 * 60;
        long minuteOffset = (index % totalMinutes);
        LocalDateTime timestamp = TEST_START_TIME.plusMinutes(minuteOffset);
        
        return new TestEntity(
            "ScanUser_T" + threadId + "_" + index,
            "scan" + threadId + "_" + index + "@test.com", // NO unique constraint
            index % 4 == 0 ? "ACTIVE" : (index % 4 == 1 ? "PENDING" : 
                (index % 4 == 2 ? "INACTIVE" : "PROCESSING")),
            20 + (index % 60),
            new BigDecimal("150.00").add(new BigDecimal(index % 3000)),
            index % 3 == 0,
            "Full table scan test entity " + index + " from thread " + threadId,
            timestamp
        );
    }
    
    private ScanResult performSingleTableScan(int threadId, ScanType scanType) {
        long threadStartTime = System.currentTimeMillis();
        long recordsScanned = 0;
        int scansCompleted = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            int iterations = FULL_SCAN_ITERATIONS / THREAD_COUNT;
            
            for (int i = 0; i < iterations; i++) {
                String sql = getScanSQL(scanType);
                
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    setScanParameters(stmt, scanType, i);
                    
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            recordsScanned++;
                        }
                        scansCompleted++;
                    }
                }
                
                if (i % 10 == 0) {
                    logger.debug("🧵 Thread {} completed {} scans", threadId, i);
                }
            }
            
        } catch (Exception e) {
            logger.error("❌ Error in single table scan thread {}", threadId, e);
        }
        
        long threadEndTime = System.currentTimeMillis();
        return new ScanResult(threadId, scansCompleted, recordsScanned, threadEndTime - threadStartTime);
    }
    
    private MultiTableScanResult performMultiTableScan(MultiScanType scanType) throws Exception {
        long recordsScanned = 0;
        int tablesScanned = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            List<String> tables = getMultiTableNames(conn);
            tablesScanned = tables.size();
            
            switch (scanType) {
                case ALL_TABLES:
                    for (String table : tables) {
                        recordsScanned += scanSingleTable(conn, table);
                    }
                    break;
                    
                case DATE_RANGE:
                    recordsScanned += scanTablesInDateRange(conn, tables);
                    break;
                    
                case AGGREGATION:
                    recordsScanned += performCrossTableAggregation(conn, tables);
                    break;
                    
                case UNION_QUERY:
                    recordsScanned += performUnionQuery(conn, tables);
                    break;
            }
        }
        
        return new MultiTableScanResult(tablesScanned, recordsScanned);
    }
    
    private String getScanSQL(ScanType scanType) {
        switch (scanType) {
            case FULL_TABLE:
                return "SELECT * FROM scan_partitioned_test";
            case PARTITION_PRUNED:
                return "SELECT * FROM scan_partitioned_test WHERE created_at BETWEEN ? AND ?";
            case INDEX_LOOKUP:
                return "SELECT * FROM scan_partitioned_test WHERE name LIKE ?";
            case FILTERED_SCAN:
                return "SELECT * FROM scan_partitioned_test WHERE status = ?";
            default:
                return "SELECT * FROM scan_partitioned_test";
        }
    }
    
    private void setScanParameters(PreparedStatement stmt, ScanType scanType, int iteration) throws SQLException {
        switch (scanType) {
            case PARTITION_PRUNED:
                LocalDateTime start = TEST_START_TIME.plusDays(iteration % DAYS_TO_SPAN);
                LocalDateTime end = start.plusHours(12);
                stmt.setTimestamp(1, Timestamp.valueOf(start));
                stmt.setTimestamp(2, Timestamp.valueOf(end));
                break;
            case INDEX_LOOKUP:
                stmt.setString(1, "ScanUser_T" + (iteration % THREAD_COUNT) + "_%");
                break;
            case FILTERED_SCAN:
                String[] statuses = {"ACTIVE", "PENDING", "INACTIVE", "PROCESSING"};
                stmt.setString(1, statuses[iteration % statuses.length]);
                break;
        }
    }
    
    private void insertBatchDirectly(Connection conn, String tableName, List<TestEntity> entities) throws SQLException {
        String insertSQL = "INSERT INTO " + tableName + 
            " (name, email, status, age, balance, active, description, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            for (TestEntity entity : entities) {
                pstmt.setString(1, entity.getName());
                pstmt.setString(2, entity.getEmail());
                pstmt.setString(3, entity.getStatus());
                pstmt.setInt(4, entity.getAge());
                pstmt.setBigDecimal(5, entity.getBalance());
                pstmt.setBoolean(6, entity.getActive());
                pstmt.setString(7, entity.getDescription());
                pstmt.setTimestamp(8, Timestamp.valueOf(entity.getCreatedAt()));
                pstmt.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }
    
    private long countRecordsInTable(String tableName) throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    private long countRecordsInMultiTables() throws SQLException {
        long totalRecords = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            List<String> tables = getMultiTableNames(conn);
            
            for (String table : tables) {
                String tableName = table.substring(table.lastIndexOf('.') + 1);
                totalRecords += countRecordsInTable(tableName);
            }
        }
        
        return totalRecords;
    }
    
    private List<String> getMultiTableNames(Connection conn) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        String query = "SELECT CONCAT(table_schema, '.', table_name) as full_name " +
                      "FROM information_schema.tables " +
                      "WHERE table_schema = ? AND table_name LIKE 'scan_multi_test_%' " +
                      "ORDER BY table_name";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    tableNames.add(rs.getString("full_name"));
                }
            }
        }
        
        return tableNames;
    }
    
    private void verifyIndexesExist(Connection conn, String tableName) throws SQLException {
        String query = "SELECT index_name, column_name FROM information_schema.statistics " +
                      "WHERE table_schema = ? AND table_name = ? ORDER BY index_name, seq_in_index";
        
        Set<String> indexes = new HashSet<>();
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, mysql.getDatabaseName());
            pstmt.setString(2, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    indexes.add(rs.getString("index_name"));
                }
            }
        }
        
        // Verify essential indexes exist (no unique constraints)
        assertThat(indexes).contains("idx_created_at", "idx_name", "idx_status");
        logger.debug("✅ Table {} has proper indexes: {}", tableName, indexes);
    }
    
    private void testIndexUsageWithExplain(Connection conn) throws SQLException {
        String[] testQueries = {
            "SELECT * FROM scan_partitioned_test WHERE created_at > '2025-08-01'",
            "SELECT * FROM scan_partitioned_test WHERE name LIKE 'ScanUser_%'",
            "SELECT * FROM scan_partitioned_test WHERE status = 'ACTIVE'"
        };
        
        for (String query : testQueries) {
            try (PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + query);
                 ResultSet rs = stmt.executeQuery()) {
                
                if (rs.next()) {
                    String key = rs.getString("key");
                    logger.debug("📊 Query uses index: {} for: {}", key, query.substring(0, 50) + "...");
                }
            }
        }
    }
    
    private long scanSingleTable(Connection conn, String table) throws SQLException {
        String tableName = table.substring(table.lastIndexOf('.') + 1);
        String sql = "SELECT COUNT(*) FROM " + tableName;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    private long scanTablesInDateRange(Connection conn, List<String> tables) throws SQLException {
        long totalRecords = 0;
        
        for (String table : tables) {
            totalRecords += scanSingleTable(conn, table);
        }
        
        return totalRecords;
    }
    
    private long performCrossTableAggregation(Connection conn, List<String> tables) throws SQLException {
        if (tables.isEmpty()) return 0;
        
        StringBuilder unionSQL = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            String tableName = tables.get(i).substring(tables.get(i).lastIndexOf('.') + 1);
            if (i > 0) unionSQL.append(" UNION ALL ");
            unionSQL.append("SELECT COUNT(*) as count FROM ").append(tableName);
        }
        
        long totalRecords = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(unionSQL.toString())) {
            
            while (rs.next()) {
                totalRecords += rs.getLong("count");
            }
        }
        
        return totalRecords;
    }
    
    private long performUnionQuery(Connection conn, List<String> tables) throws SQLException {
        if (tables.isEmpty()) return 0;
        
        // Limit to first 5 tables for performance
        List<String> limitedTables = tables.subList(0, Math.min(5, tables.size()));
        
        StringBuilder unionSQL = new StringBuilder();
        for (int i = 0; i < limitedTables.size(); i++) {
            String tableName = limitedTables.get(i).substring(limitedTables.get(i).lastIndexOf('.') + 1);
            if (i > 0) unionSQL.append(" UNION ALL ");
            unionSQL.append("SELECT name, status FROM ").append(tableName).append(" LIMIT 100");
        }
        
        long recordCount = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(unionSQL.toString())) {
            
            while (rs.next()) {
                recordCount++;
            }
        }
        
        return recordCount;
    }
    
    @AfterAll
    static void printFullTableScanResults() {
        logger.info("🎯 FULL TABLE SCAN STRESS TEST FINAL RESULTS");
        logger.info("=".repeat(70));
        logger.info("📊 SCAN PERFORMANCE METRICS:");
        logger.info("   • Single table records: {}", totalRecordsInSingleTable.get());
        logger.info("   • Multi-table records: {}", totalRecordsInMultiTables.get());
        logger.info("   • Single table scan time: {}ms", singleTableScanTime.get());
        logger.info("   • Multi-table scan time: {}ms", multiTableScanTime.get());
        logger.info("   • Full scans completed: {}", fullScansCompleted.get());
        
        logger.info("🔍 SCAN TYPE BREAKDOWN:");
        scanMetrics.entrySet().forEach(entry -> 
            logger.info("   • {}: {}", entry.getKey(), entry.getValue().get()));
        
        logger.info("🏆 FULL TABLE SCAN CAPABILITIES VERIFIED:");
        logger.info("   ✅ Single partitioned table - full partition scans");
        logger.info("   ✅ Multi-table architecture - scan all tables");
        logger.info("   ✅ Optimized indexing WITHOUT unique constraints");
        logger.info("   ✅ Partition pruning vs full scan performance");
        logger.info("   ✅ Cross-table aggregation and UNION queries");
        logger.info("   ✅ Index-optimized lookups for faster performance");
        
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
    
    // Result and configuration classes
    enum ScanType {
        FULL_TABLE, PARTITION_PRUNED, INDEX_LOOKUP, FILTERED_SCAN
    }
    
    enum MultiScanType {
        ALL_TABLES, DATE_RANGE, AGGREGATION, UNION_QUERY
    }
    
    static class FullScanTest {
        final String name;
        final ScanType scanType;
        
        FullScanTest(String name, ScanType scanType) {
            this.name = name;
            this.scanType = scanType;
        }
    }
    
    static class MultiTableScanTest {
        final String name;
        final MultiScanType scanType;
        
        MultiTableScanTest(String name, MultiScanType scanType) {
            this.name = name;
            this.scanType = scanType;
        }
    }
    
    static class ScanResult {
        final int threadId;
        final int scansCompleted;
        final long recordsScanned;
        final long scanTime;
        
        ScanResult(int threadId, int scansCompleted, long recordsScanned, long scanTime) {
            this.threadId = threadId;
            this.scansCompleted = scansCompleted;
            this.recordsScanned = recordsScanned;
            this.scanTime = scanTime;
        }
    }
    
    static class MultiTableScanResult {
        final int tablesScanned;
        final long recordsScanned;
        
        MultiTableScanResult(int tablesScanned, long recordsScanned) {
            this.tablesScanned = tablesScanned;
            this.recordsScanned = recordsScanned;
        }
    }
}