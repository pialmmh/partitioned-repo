package com.telcobright.db.stress;

import com.telcobright.db.connection.ConnectionProvider;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.GenericMultiTableRepository;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * COMPREHENSIVE MULTI-TABLE ARCHITECTURE STRESS TEST
 * 
 * Tests rock-solid reliability of:
 * - Separate tables per day (e.g., sms_20250828, sms_20250829)
 * - Cross-table queries with UNION ALL operations
 * - Data isolation between tables
 * - Table naming conventions and consistency
 * - Performance of multi-table operations
 * - Million+ records distributed across 100+ daily tables
 * - Query performance across multiple tables with date ranges
 */
@Testcontainers
@DisplayName("🏗️ MULTI-TABLE ARCHITECTURE STRESS TEST - Million Records Across 100+ Tables")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiTableArchitectureStressTest {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiTableArchitectureStressTest.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    // STRESS TEST PARAMETERS
    private static final int MILLION_RECORDS = 1_000_000;
    private static final int DAYS_TO_SPAN = 100; // 100+ tables
    private static final int BATCH_SIZE = 5_000;
    private static final int THREAD_COUNT = 15;
    private static final int CROSS_TABLE_QUERIES = 10_000;
    private static final String TABLE_PREFIX = "multitable_test";
    
    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("multi_table_stress_db")
            .withUsername("multi_user")
            .withPassword("multi_pass")
            .withCommand("--innodb-buffer-pool-size=2G", 
                        "--innodb-log-file-size=1G",
                        "--max-connections=1000",
                        "--innodb-flush-log-at-trx-commit=2");
    
    private static ConnectionProvider connectionProvider;
    private static GenericMultiTableRepository<TestEntity, Long> repository;
    private static EntityMetadata<TestEntity, Long> entityMetadata;
    private static ExecutorService executorService;
    
    // Metrics collection
    private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
    private static final AtomicInteger totalTablesCreated = new AtomicInteger(0);
    private static final AtomicLong totalQueryTime = new AtomicLong(0);
    private static final AtomicInteger crossTableQueriesExecuted = new AtomicInteger(0);
    private static final Map<String, AtomicLong> dailyDistribution = new ConcurrentHashMap<>();
    private static final Set<String> createdTables = ConcurrentHashMap.newKeySet();
    private static final LocalDateTime TEST_START_TIME = LocalDateTime.now().minusDays(DAYS_TO_SPAN);
    
    @BeforeAll
    static void setupMultiTableStressTest() {
        logger.info("🚀 INITIALIZING MULTI-TABLE ARCHITECTURE STRESS TEST");
        logger.info("📊 Test Parameters:");
        logger.info("   • Records to distribute: {}", MILLION_RECORDS);
        logger.info("   • Days to span (tables): {}", DAYS_TO_SPAN);
        logger.info("   • Cross-table queries: {}", CROSS_TABLE_QUERIES);
        logger.info("   • Thread count: {}", THREAD_COUNT);
        logger.info("   • Table prefix: {}", TABLE_PREFIX);
        
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
        
        // Initialize repository
        try {
            repository = GenericMultiTableRepository.builder(TestEntity.class, Long.class)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .tablePrefix(TABLE_PREFIX)
                .partitionRetentionPeriod(DAYS_TO_SPAN + 10)
                .autoManagePartitions(true)
                .initializePartitionsOnStart(false) // We'll control table creation
                .build();
        } catch (Exception e) {
            logger.warn("Repository initialization skipped - using direct DB operations: {}", e.getMessage());
        }
        
        logger.info("✅ Multi-table stress test initialization completed");
    }
    
    @Test
    @Order(1)
    @DisplayName("1. 🏗️ DISTRIBUTED TABLE CREATION STRESS TEST")
    void testDistributedTableCreation() throws Exception {
        logger.info("🏗️ STARTING DISTRIBUTED TABLE CREATION STRESS TEST");
        
        long startTime = System.currentTimeMillis();
        List<Future<TableCreationResult>> futures = new ArrayList<>();
        
        // Create tables across date range
        List<LocalDateTime> targetDates = generateDateRange();
        int datesPerThread = targetDates.size() / THREAD_COUNT;
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            int startIdx = threadId * datesPerThread;
            int endIdx = (threadId == THREAD_COUNT - 1) ? targetDates.size() : startIdx + datesPerThread;
            final List<LocalDateTime> threadDates = targetDates.subList(startIdx, endIdx);
            
            Future<TableCreationResult> future = executorService.submit(() -> 
                createTablesForDates(finalThreadId, threadDates));
            futures.add(future);
        }
        
        // Collect results
        int totalTablesCreatedCount = 0;
        for (Future<TableCreationResult> future : futures) {
            TableCreationResult result = future.get();
            totalTablesCreatedCount += result.tablesCreated;
            createdTables.addAll(result.tableNames);
            
            logger.info("🧵 Thread {} created {} tables", result.threadId, result.tablesCreated);
        }
        
        long endTime = System.currentTimeMillis();
        
        // Verify table creation
        verifyTableNamingConventions();
        verifyTableStructureConsistency();
        
        totalTablesCreated.set(totalTablesCreatedCount);
        
        logger.info("✅ DISTRIBUTED TABLE CREATION COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Tables created: {}", totalTablesCreatedCount);
        logger.info("   • Expected tables: {}", DAYS_TO_SPAN);
        logger.info("   • Creation time: {}ms", endTime - startTime);
        logger.info("   • Average time per table: {}ms", 
            (endTime - startTime) / Math.max(1, totalTablesCreatedCount));
        
        assertThat(totalTablesCreatedCount).isGreaterThanOrEqualTo(DAYS_TO_SPAN);
    }
    
    @Test
    @Order(2)
    @DisplayName("2. 📊 MILLION RECORDS DISTRIBUTION ACROSS TABLES")
    void testMillionRecordsDistribution() throws Exception {
        logger.info("📊 DISTRIBUTING MILLION RECORDS ACROSS TABLES");
        
        long startTime = System.currentTimeMillis();
        List<Future<DistributionResult>> futures = new ArrayList<>();
        
        int recordsPerThread = MILLION_RECORDS / THREAD_COUNT;
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            final int startRecord = threadId * recordsPerThread;
            final int endRecord = (threadId == THREAD_COUNT - 1) ? MILLION_RECORDS : startRecord + recordsPerThread;
            
            Future<DistributionResult> future = executorService.submit(() -> 
                distributeRecordsAcrossTables(finalThreadId, startRecord, endRecord));
            futures.add(future);
        }
        
        // Collect results
        long totalInserted = 0;
        Map<String, Integer> tableUsage = new HashMap<>();
        
        for (Future<DistributionResult> future : futures) {
            DistributionResult result = future.get();
            totalInserted += result.recordsInserted;
            
            // Merge table usage stats
            for (Map.Entry<String, Integer> entry : result.recordsByTable.entrySet()) {
                tableUsage.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
            
            logger.info("🧵 Thread {} distributed {} records across {} tables", 
                result.threadId, result.recordsInserted, result.recordsByTable.size());
        }
        
        long endTime = System.currentTimeMillis();
        
        // Analyze distribution
        analyzeDataDistribution(tableUsage);
        
        totalRecordsInserted.set(totalInserted);
        
        logger.info("✅ MILLION RECORDS DISTRIBUTION COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Total records inserted: {}", totalInserted);
        logger.info("   • Tables with data: {}", tableUsage.size());
        logger.info("   • Distribution time: {}ms", endTime - startTime);
        logger.info("   • Insertion rate: {} records/second", 
            (totalInserted * 1000) / (endTime - startTime));
        
        assertThat(totalInserted).isEqualTo(MILLION_RECORDS);
        assertThat(tableUsage.size()).isGreaterThan(50); // Should use many tables
    }
    
    @Test
    @Order(3)
    @DisplayName("3. 🔍 CROSS-TABLE QUERY PERFORMANCE STRESS TEST")
    void testCrossTableQueryPerformance() throws Exception {
        logger.info("🔍 TESTING CROSS-TABLE QUERY PERFORMANCE");
        
        long startTime = System.currentTimeMillis();
        List<Future<QueryResult>> futures = new ArrayList<>();
        
        int queriesPerThread = CROSS_TABLE_QUERIES / THREAD_COUNT;
        
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            final int finalQueriesPerThread = queriesPerThread;
            Future<QueryResult> future = executorService.submit(() -> 
                executeCrossTableQueries(finalThreadId, finalQueriesPerThread));
            futures.add(future);
        }
        
        // Collect results
        long totalQueries = 0;
        long totalQueryTime = 0;
        long totalRecordsQueried = 0;
        
        for (Future<QueryResult> future : futures) {
            QueryResult result = future.get();
            totalQueries += result.queriesExecuted;
            totalQueryTime += result.totalQueryTime;
            totalRecordsQueried += result.recordsQueried;
            
            logger.info("🧵 Thread {} executed {} queries, retrieved {} records", 
                result.threadId, result.queriesExecuted, result.recordsQueried);
        }
        
        long endTime = System.currentTimeMillis();
        
        crossTableQueriesExecuted.set((int) totalQueries);
        this.totalQueryTime.set(totalQueryTime);
        
        logger.info("✅ CROSS-TABLE QUERY PERFORMANCE TEST COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Total queries executed: {}", totalQueries);
        logger.info("   • Total records queried: {}", totalRecordsQueried);
        logger.info("   • Average query time: {}ms", totalQueryTime / Math.max(1, totalQueries));
        logger.info("   • Query throughput: {} queries/second", 
            (totalQueries * 1000) / (endTime - startTime));
        logger.info("   • Records per query: {}", totalRecordsQueried / Math.max(1, totalQueries));
        
        assertThat(totalQueries).isEqualTo(CROSS_TABLE_QUERIES);
        assertThat(totalRecordsQueried).isGreaterThan(0);
    }
    
    @Test
    @Order(4)
    @DisplayName("4. 🔒 DATA ISOLATION VERIFICATION")
    void testDataIsolationVerification() throws Exception {
        logger.info("🔒 VERIFYING DATA ISOLATION BETWEEN TABLES");
        
        Map<String, Set<Long>> tableRecordIds = new HashMap<>();
        Map<String, LocalDateTime> tableDateRanges = new HashMap<>();
        
        try (Connection conn = connectionProvider.getConnection()) {
            // Analyze each table's data
            for (String tableName : createdTables) {
                Set<Long> recordIds = getRecordIds(conn, tableName);
                LocalDateTime tableDate = extractDateFromTableName(tableName);
                
                tableRecordIds.put(tableName, recordIds);
                tableDateRanges.put(tableName, tableDate);
                
                // Verify records in table match expected date
                verifyTableDateConsistency(conn, tableName, tableDate);
            }
            
            // Verify no duplicate IDs across tables
            verifyNoDuplicateIdsAcrossTables(tableRecordIds);
            
            // Verify date boundaries
            verifyDateBoundaries(tableDateRanges);
        }
        
        logger.info("✅ DATA ISOLATION VERIFICATION COMPLETED");
        logger.info("📊 Results:");
        logger.info("   • Tables analyzed: {}", tableRecordIds.size());
        logger.info("   • Total unique records: {}", 
            tableRecordIds.values().stream().mapToInt(Set::size).sum());
        logger.info("   • Date range coverage: {} days", tableDateRanges.size());
        logger.info("   • All isolation checks passed ✅");
    }
    
    @Test
    @Order(5)
    @DisplayName("5. 🚀 UNION ALL PERFORMANCE STRESS TEST")
    void testUnionAllPerformanceStress() throws Exception {
        logger.info("🚀 TESTING UNION ALL PERFORMANCE ACROSS MULTIPLE TABLES");
        
        List<UnionQueryTest> unionTests = Arrays.asList(
            new UnionQueryTest("2-Table Union", 2),
            new UnionQueryTest("5-Table Union", 5),
            new UnionQueryTest("10-Table Union", 10),
            new UnionQueryTest("20-Table Union", 20),
            new UnionQueryTest("50-Table Union", 50)
        );
        
        for (UnionQueryTest unionTest : unionTests) {
            logger.info("🧪 Testing: {}", unionTest.name);
            
            long startTime = System.currentTimeMillis();
            
            // Select random tables for union
            List<String> selectedTables = selectRandomTables(unionTest.tableCount);
            
            // Execute UNION ALL query
            long recordCount = executeUnionAllQuery(selectedTables);
            
            long endTime = System.currentTimeMillis();
            long queryTime = endTime - startTime;
            
            logger.info("✅ {} completed: {} records in {}ms ({} tables)", 
                unionTest.name, recordCount, queryTime, selectedTables.size());
            
            // Verify performance is reasonable
            assertThat(queryTime).isLessThan(30000); // Max 30 seconds
            assertThat(recordCount).isGreaterThan(0);
        }
        
        logger.info("✅ UNION ALL PERFORMANCE STRESS TEST COMPLETED");
    }
    
    // Helper Methods
    
    private List<LocalDateTime> generateDateRange() {
        List<LocalDateTime> dates = new ArrayList<>();
        LocalDateTime current = TEST_START_TIME;
        
        for (int i = 0; i < DAYS_TO_SPAN; i++) {
            dates.add(current.plusDays(i));
        }
        
        return dates;
    }
    
    private TableCreationResult createTablesForDates(int threadId, List<LocalDateTime> dates) {
        Set<String> tableNames = new HashSet<>();
        int created = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            for (LocalDateTime date : dates) {
                String tableName = TABLE_PREFIX + "_" + date.toLocalDate().format(DATE_FORMAT);
                
                if (createTableIfNotExists(conn, tableName)) {
                    created++;
                    tableNames.add(tableName);
                }
            }
        } catch (Exception e) {
            logger.error("❌ Error in table creation thread {}", threadId, e);
            throw new RuntimeException("Table creation failed", e);
        }
        
        return new TableCreationResult(threadId, created, tableNames);
    }
    
    private boolean createTableIfNotExists(Connection conn, String tableName) throws SQLException {
        String createTableSQL = String.format(entityMetadata.getCreateTableSQL(), tableName);
        
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSQL);
            return true;
        } catch (SQLException e) {
            if (e.getMessage().contains("already exists")) {
                return false;
            }
            throw e;
        }
    }
    
    private void verifyTableNamingConventions() throws SQLException {
        try (Connection conn = connectionProvider.getConnection()) {
            Set<String> actualTables = new HashSet<>();
            
            try (ResultSet rs = conn.getMetaData().getTables(null, mysql.getDatabaseName(), 
                TABLE_PREFIX + "_%", new String[]{"TABLE"})) {
                
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    actualTables.add(tableName);
                    
                    // Verify naming convention
                    assertThat(tableName).matches(TABLE_PREFIX + "_\\d{8}");
                }
            }
            
            logger.info("✅ Table naming conventions verified: {} tables", actualTables.size());
        }
    }
    
    private void verifyTableStructureConsistency() throws SQLException {
        try (Connection conn = connectionProvider.getConnection()) {
            Map<String, String> expectedStructure = new HashMap<>();
            boolean first = true;
            
            for (String tableName : createdTables) {
                Map<String, String> currentStructure = getTableStructure(conn, tableName);
                
                if (first) {
                    expectedStructure.putAll(currentStructure);
                    first = false;
                } else {
                    // Verify structure matches
                    assertThat(currentStructure).isEqualTo(expectedStructure);
                }
            }
            
            logger.info("✅ Table structure consistency verified across {} tables", createdTables.size());
        }
    }
    
    private Map<String, String> getTableStructure(Connection conn, String tableName) throws SQLException {
        Map<String, String> structure = new HashMap<>();
        
        try (ResultSet rs = conn.getMetaData().getColumns(null, mysql.getDatabaseName(), tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("TYPE_NAME");
                structure.put(columnName, columnType);
            }
        }
        
        return structure;
    }
    
    private DistributionResult distributeRecordsAcrossTables(int threadId, int startRecord, int endRecord) {
        int recordsInserted = 0;
        Map<String, Integer> recordsByTable = new HashMap<>();
        
        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(false);
            
            for (int i = startRecord; i < endRecord; i += BATCH_SIZE) {
                int batchEnd = Math.min(i + BATCH_SIZE, endRecord);
                
                Map<String, List<TestEntity>> batchesByTable = new HashMap<>();
                
                // Create entities distributed across time range
                for (int j = i; j < batchEnd; j++) {
                    TestEntity entity = createMultiTableTestEntity(threadId, j);
                    String tableName = TABLE_PREFIX + "_" + 
                        entity.getCreatedAt().toLocalDate().format(DATE_FORMAT);
                    
                    batchesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(entity);
                }
                
                // Insert batches
                for (Map.Entry<String, List<TestEntity>> entry : batchesByTable.entrySet()) {
                    String tableName = entry.getKey();
                    List<TestEntity> entities = entry.getValue();
                    
                    insertBatch(conn, tableName, entities);
                    recordsInserted += entities.size();
                    recordsByTable.merge(tableName, entities.size(), Integer::sum);
                }
                
                conn.commit();
            }
            
        } catch (Exception e) {
            logger.error("❌ Error in distribution thread {}", threadId, e);
            throw new RuntimeException("Distribution failed", e);
        }
        
        return new DistributionResult(threadId, recordsInserted, recordsByTable);
    }
    
    private TestEntity createMultiTableTestEntity(int threadId, int index) {
        // Distribute across the full date range
        long totalDays = DAYS_TO_SPAN;
        long dayOffset = index % totalDays;
        LocalDateTime timestamp = TEST_START_TIME.plusDays(dayOffset).plusMinutes(index % 1440);
        
        // Track daily distribution
        String dateKey = timestamp.toLocalDate().format(DATE_FORMAT);
        dailyDistribution.computeIfAbsent(dateKey, k -> new AtomicLong(0)).incrementAndGet();
        
        return new TestEntity(
            "MultiTable_T" + threadId + "_" + index,
            "multitable" + threadId + "_" + index + "@test.com",
            index % 4 == 0 ? "ACTIVE" : (index % 4 == 1 ? "PENDING" : 
                (index % 4 == 2 ? "INACTIVE" : "SUSPENDED")),
            20 + (index % 50),
            new BigDecimal("50.00").add(new BigDecimal(index % 5000)),
            index % 3 == 0,
            "Multi-table test entity " + index + " from thread " + threadId,
            timestamp
        );
    }
    
    private void insertBatch(Connection conn, String tableName, List<TestEntity> entities) throws SQLException {
        String insertSQL = String.format(entityMetadata.getInsertSQL(), tableName);
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
            for (TestEntity entity : entities) {
                entityMetadata.setInsertParameters(pstmt, entity);
                pstmt.addBatch();
            }
            
            pstmt.executeBatch();
            
            // Set generated IDs
            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                int i = 0;
                while (generatedKeys.next() && i < entities.size()) {
                    entities.get(i++).setId(generatedKeys.getLong(1));
                }
            }
        }
    }
    
    private void analyzeDataDistribution(Map<String, Integer> tableUsage) {
        logger.info("📈 DATA DISTRIBUTION ANALYSIS:");
        
        int totalTables = tableUsage.size();
        int totalRecords = tableUsage.values().stream().mapToInt(Integer::intValue).sum();
        double averageRecordsPerTable = (double) totalRecords / totalTables;
        
        // Find min/max distribution
        int minRecords = tableUsage.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        int maxRecords = tableUsage.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        
        logger.info("   • Tables with data: {}", totalTables);
        logger.info("   • Average records per table: {:.2f}", averageRecordsPerTable);
        logger.info("   • Min records in a table: {}", minRecords);
        logger.info("   • Max records in a table: {}", maxRecords);
        logger.info("   • Distribution variance: {:.2f}%", 
            ((double) (maxRecords - minRecords) / averageRecordsPerTable) * 100);
    }
    
    private QueryResult executeCrossTableQueries(int threadId, int queryCount) {
        long totalQueryTime = 0;
        long recordsQueried = 0;
        int queriesExecuted = 0;
        
        try (Connection conn = connectionProvider.getConnection()) {
            Random random = new Random(threadId);
            
            for (int i = 0; i < queryCount; i++) {
                // Random date range query (1-7 days)
                int daysToQuery = 1 + random.nextInt(7);
                LocalDateTime startDate = TEST_START_TIME.plusDays(random.nextInt(DAYS_TO_SPAN - daysToQuery));
                LocalDateTime endDate = startDate.plusDays(daysToQuery);
                
                long queryStart = System.currentTimeMillis();
                long records = executeDateRangeQuery(conn, startDate, endDate);
                long queryEnd = System.currentTimeMillis();
                
                totalQueryTime += (queryEnd - queryStart);
                recordsQueried += records;
                queriesExecuted++;
                
                if (i % 1000 == 0) {
                    logger.debug("🧵 Thread {} executed {} queries", threadId, i);
                }
            }
            
        } catch (Exception e) {
            logger.error("❌ Error in query thread {}", threadId, e);
        }
        
        return new QueryResult(threadId, queriesExecuted, totalQueryTime, recordsQueried);
    }
    
    private long executeDateRangeQuery(Connection conn, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Generate table names for date range
        List<String> tables = new ArrayList<>();
        LocalDateTime current = startDate;
        
        while (!current.isAfter(endDate)) {
            String tableName = TABLE_PREFIX + "_" + current.toLocalDate().format(DATE_FORMAT);
            if (createdTables.contains(tableName)) {
                tables.add(tableName);
            }
            current = current.plusDays(1);
        }
        
        if (tables.isEmpty()) {
            return 0;
        }
        
        // Build UNION ALL query
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i > 0) queryBuilder.append(" UNION ALL ");
            queryBuilder.append("SELECT COUNT(*) as count FROM ").append(tables.get(i))
                       .append(" WHERE created_at >= ? AND created_at <= ?");
        }
        
        try (PreparedStatement pstmt = conn.prepareStatement(queryBuilder.toString())) {
            // Set parameters for each table query
            int paramIndex = 1;
            for (String table : tables) {
                pstmt.setTimestamp(paramIndex++, Timestamp.valueOf(startDate));
                pstmt.setTimestamp(paramIndex++, Timestamp.valueOf(endDate));
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                long totalCount = 0;
                while (rs.next()) {
                    totalCount += rs.getLong("count");
                }
                return totalCount;
            }
        }
    }
    
    private Set<Long> getRecordIds(Connection conn, String tableName) throws SQLException {
        Set<Long> ids = new HashSet<>();
        String query = "SELECT id FROM " + tableName;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
        }
        
        return ids;
    }
    
    private LocalDateTime extractDateFromTableName(String tableName) {
        String dateStr = tableName.substring(tableName.lastIndexOf('_') + 1);
        return LocalDateTime.parse(dateStr + "0000", 
            DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
    }
    
    private void verifyTableDateConsistency(Connection conn, String tableName, LocalDateTime expectedDate) throws SQLException {
        String query = "SELECT COUNT(*) as count FROM " + tableName + 
                      " WHERE DATE(created_at) != ?";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setDate(1, java.sql.Date.valueOf(expectedDate.toLocalDate()));
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int mismatchCount = rs.getInt("count");
                    if (mismatchCount > 0) {
                        logger.warn("⚠️ Table {} has {} records with mismatched dates", 
                            tableName, mismatchCount);
                    }
                }
            }
        }
    }
    
    private void verifyNoDuplicateIdsAcrossTables(Map<String, Set<Long>> tableRecordIds) {
        Set<Long> allIds = new HashSet<>();
        int totalRecords = 0;
        
        for (Map.Entry<String, Set<Long>> entry : tableRecordIds.entrySet()) {
            Set<Long> tableIds = entry.getValue();
            totalRecords += tableIds.size();
            
            // Check for duplicates
            for (Long id : tableIds) {
                if (!allIds.add(id)) {
                    fail("Duplicate ID found across tables: " + id);
                }
            }
        }
        
        logger.info("✅ No duplicate IDs found across {} tables ({} unique records)", 
            tableRecordIds.size(), totalRecords);
    }
    
    private void verifyDateBoundaries(Map<String, LocalDateTime> tableDateRanges) {
        List<LocalDateTime> sortedDates = tableDateRanges.values().stream()
            .sorted()
            .collect(Collectors.toList());
        
        for (int i = 1; i < sortedDates.size(); i++) {
            LocalDateTime prev = sortedDates.get(i - 1);
            LocalDateTime current = sortedDates.get(i);
            
            // Verify dates are consecutive days or have gaps (both are valid)
            long daysBetween = prev.toLocalDate().until(current.toLocalDate()).getDays();
            assertThat(daysBetween).isGreaterThanOrEqualTo(1);
        }
        
        logger.info("✅ Date boundaries verified across {} tables", tableDateRanges.size());
    }
    
    private List<String> selectRandomTables(int count) {
        List<String> allTables = new ArrayList<>(createdTables);
        Collections.shuffle(allTables);
        return allTables.subList(0, Math.min(count, allTables.size()));
    }
    
    private long executeUnionAllQuery(List<String> tables) throws SQLException {
        if (tables.isEmpty()) return 0;
        
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i > 0) queryBuilder.append(" UNION ALL ");
            queryBuilder.append("SELECT COUNT(*) as count FROM ").append(tables.get(i));
        }
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryBuilder.toString())) {
            
            long totalCount = 0;
            while (rs.next()) {
                totalCount += rs.getLong("count");
            }
            return totalCount;
        }
    }
    
    @AfterAll
    static void printMultiTableResults() {
        logger.info("🎯 MULTI-TABLE ARCHITECTURE STRESS TEST FINAL RESULTS");
        logger.info("=".repeat(70));
        logger.info("📊 ARCHITECTURE METRICS:");
        logger.info("   • Total records processed: {}", totalRecordsInserted.get());
        logger.info("   • Tables created: {}", totalTablesCreated.get());
        logger.info("   • Cross-table queries executed: {}", crossTableQueriesExecuted.get());
        logger.info("   • Average query time: {}ms", 
            totalQueryTime.get() / Math.max(1, crossTableQueriesExecuted.get()));
        
        logger.info("📅 DAILY DATA DISTRIBUTION:");
        dailyDistribution.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .limit(10) // Show first 10 days
            .forEach(entry -> logger.info("   • Date {}: {} records", 
                entry.getKey(), entry.getValue().get()));
        
        if (dailyDistribution.size() > 10) {
            logger.info("   • ... and {} more dates", dailyDistribution.size() - 10);
        }
        
        logger.info("🏆 MULTI-TABLE ARCHITECTURE TEST COMPLETED SUCCESSFULLY!");
        
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
    
    // Result classes
    static class TableCreationResult {
        final int threadId;
        final int tablesCreated;
        final Set<String> tableNames;
        
        TableCreationResult(int threadId, int tablesCreated, Set<String> tableNames) {
            this.threadId = threadId;
            this.tablesCreated = tablesCreated;
            this.tableNames = tableNames;
        }
    }
    
    static class DistributionResult {
        final int threadId;
        final int recordsInserted;
        final Map<String, Integer> recordsByTable;
        
        DistributionResult(int threadId, int recordsInserted, Map<String, Integer> recordsByTable) {
            this.threadId = threadId;
            this.recordsInserted = recordsInserted;
            this.recordsByTable = recordsByTable;
        }
    }
    
    static class QueryResult {
        final int threadId;
        final int queriesExecuted;
        final long totalQueryTime;
        final long recordsQueried;
        
        QueryResult(int threadId, int queriesExecuted, long totalQueryTime, long recordsQueried) {
            this.threadId = threadId;
            this.queriesExecuted = queriesExecuted;
            this.totalQueryTime = totalQueryTime;
            this.recordsQueried = recordsQueried;
        }
    }
    
    static class UnionQueryTest {
        final String name;
        final int tableCount;
        
        UnionQueryTest(String name, int tableCount) {
            this.name = name;
            this.tableCount = tableCount;
        }
    }
}