package com.telcobright.splitverse.tests;

import com.telcobright.core.annotation.*;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.SimpleSequentialRepository;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for SimpleSequentialRepository with automatic ID management
 */
public class SimpleSequentialRepositoryTest {

    private static final String TEST_DB = "seq_test_db";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    private SimpleSequentialRepository<SequentialLog> repository;
    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        // Create test database
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false", HOST, PORT);
        connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            stmt.execute("CREATE DATABASE " + TEST_DB);
        }

        System.out.println("\n=== Simple Sequential Repository Test ===");
        System.out.println("Database: " + TEST_DB);
    }

    @Test
    void testSequentialIdGeneration() throws SQLException {
        System.out.println("\n=== Test 1: Sequential ID Generation ===");

        // Initialize repository with max ID of 1000
        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(1000)
            .wrapAround(true)
            .retentionDays(7)
            .build();

        // Test single ID generation
        long id1 = repository.getNextId();
        long id2 = repository.getNextId();
        long id3 = repository.getNextId();

        assertEquals(1, id1, "First ID should be 1");
        assertEquals(2, id2, "Second ID should be 2");
        assertEquals(3, id3, "Third ID should be 3");
        System.out.println("✓ Sequential IDs generated: " + id1 + ", " + id2 + ", " + id3);

        // Test bulk ID generation
        List<Long> bulkIds = repository.getNextN(10);
        assertEquals(10, bulkIds.size());
        assertEquals(4, bulkIds.get(0), "First bulk ID should be 4");
        assertEquals(13, bulkIds.get(9), "Last bulk ID should be 13");
        System.out.println("✓ Bulk IDs generated: " + bulkIds);

        // Check current ID
        assertEquals(13, repository.getCurrentId(), "Current ID should be 13");
        System.out.println("✓ Current ID: " + repository.getCurrentId());
    }

    @Test
    void testIdWraparound() throws SQLException {
        System.out.println("\n=== Test 2: ID Wraparound ===");

        // Initialize with small max ID to test wraparound
        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10)
            .wrapAround(true)
            .retentionDays(7)
            .build();

        // Generate IDs up to max
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ids.add(repository.getNextId());
        }
        System.out.println("IDs before wraparound: " + ids);
        assertEquals(10, ids.get(9), "Last ID should be 10 (max)");

        // Next ID should wrap to 1
        long wrappedId = repository.getNextId();
        assertEquals(1, wrappedId, "Should wrap around to 1");
        System.out.println("✓ ID wrapped around to: " + wrappedId);

        // Test bulk generation across wraparound
        List<Long> wrappedBulk = repository.getNextN(5);
        System.out.println("✓ Bulk IDs after wraparound: " + wrappedBulk);
        assertEquals(Arrays.asList(2L, 3L, 4L, 5L, 6L), wrappedBulk);
    }

    @Test
    void testNoWraparoundMode() throws SQLException {
        System.out.println("\n=== Test 3: No Wraparound Mode ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(5)
            .wrapAround(false)  // Disable wraparound
            .retentionDays(7)
            .build();

        // Generate IDs up to max
        for (int i = 0; i < 5; i++) {
            repository.getNextId();
        }
        System.out.println("✓ Generated IDs up to max (5)");

        // Next ID should throw exception
        assertThrows(IllegalStateException.class, () -> repository.getNextId(),
            "Should throw exception when max reached without wraparound");
        System.out.println("✓ Exception thrown when max reached without wraparound");

        // Bulk request that exceeds max should also fail
        repository.resetId(3); // Reset to 3
        assertThrows(IllegalStateException.class, () -> repository.getNextN(5),
            "Bulk request exceeding max should fail");
        System.out.println("✓ Bulk request exceeding max correctly failed");
    }

    @Test
    void testAutoIdInsertion() throws SQLException {
        System.out.println("\n=== Test 4: Auto ID Insertion ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .retentionDays(7)
            .build();

        // Insert single log with auto ID
        SequentialLog log1 = new SequentialLog();
        log1.setTimestamp(LocalDateTime.now());
        log1.setMessage("Test log 1");
        log1.setLevel("INFO");

        long assignedId1 = repository.insertWithAutoId(log1);
        assertEquals(1, assignedId1, "First auto ID should be 1");
        System.out.println("✓ Inserted log with auto ID: " + assignedId1);

        // Insert multiple logs with auto IDs
        List<SequentialLog> logs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            SequentialLog log = new SequentialLog();
            log.setTimestamp(LocalDateTime.now().plusMinutes(i));
            log.setMessage("Bulk log " + i);
            log.setLevel("DEBUG");
            logs.add(log);
        }

        List<Long> assignedIds = repository.insertMultipleWithAutoId(logs);
        assertEquals(Arrays.asList(2L, 3L, 4L, 5L, 6L), assignedIds);
        System.out.println("✓ Bulk inserted with IDs: " + assignedIds);

        // Verify logs can be retrieved
        SequentialLog retrieved = repository.findById(1);
        assertNotNull(retrieved);
        assertEquals("Test log 1", retrieved.getMessage());
        System.out.println("✓ Retrieved log by ID: " + retrieved.getMessage());
    }

    @Test
    void testClientProvidedIds() throws SQLException {
        System.out.println("\n=== Test 5: Client Provided IDs ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .allowClientIds(true)  // Enable client IDs
            .retentionDays(7)
            .build();

        // Insert with client-provided ID
        SequentialLog log1 = new SequentialLog();
        log1.setTimestamp(LocalDateTime.now());
        log1.setMessage("Client ID log");
        log1.setLevel("WARN");

        repository.insertWithClientId(log1, 100);
        System.out.println("✓ Inserted with client ID: 100");

        // Verify internal counter updated
        assertEquals(100, repository.getCurrentId(), "Counter should update to client ID");

        // Next auto ID should be 101
        long nextId = repository.getNextId();
        assertEquals(101, nextId, "Next ID should be 101");
        System.out.println("✓ Next auto ID after client ID: " + nextId);

        // Test bulk client IDs
        List<SequentialLog> logs = new ArrayList<>();
        List<Long> clientIds = new ArrayList<>();
        for (int i = 200; i <= 205; i++) {
            SequentialLog log = new SequentialLog();
            log.setTimestamp(LocalDateTime.now());
            log.setMessage("Client batch " + i);
            log.setLevel("INFO");
            logs.add(log);
            clientIds.add((long) i);
        }

        repository.insertMultipleWithClientIds(logs, clientIds);
        System.out.println("✓ Bulk inserted with client IDs: 200-205");

        // Verify counter updated to highest
        assertEquals(205, repository.getCurrentId());
        System.out.println("✓ Counter updated to highest client ID: 205");
    }

    @Test
    void testIdRangeOperations() throws SQLException {
        System.out.println("\n=== Test 6: ID Range Operations ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .allowClientIds(true)
            .retentionDays(7)
            .build();

        // Reserve ID range
        SimpleSequentialRepository.IdRange range1 = repository.reserveIdRange(100);
        assertEquals(1, range1.getStart());
        assertEquals(100, range1.getEnd());
        assertEquals(100, range1.getCount());
        System.out.println("✓ Reserved range: " + range1.getStart() + "-" + range1.getEnd());

        // Verify counter advanced
        assertEquals(100, repository.getCurrentId());

        // Reserve another range
        SimpleSequentialRepository.IdRange range2 = repository.reserveIdRange(50);
        assertEquals(101, range2.getStart());
        assertEquals(150, range2.getEnd());
        System.out.println("✓ Reserved second range: " + range2.getStart() + "-" + range2.getEnd());

        // Insert with reserved range
        List<SequentialLog> logs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            SequentialLog log = new SequentialLog();
            log.setTimestamp(LocalDateTime.now());
            log.setMessage("Range log " + i);
            log.setLevel("INFO");
            logs.add(log);
        }

        repository.insertWithIdRange(logs, range2);
        System.out.println("✓ Inserted 50 logs with reserved range");

        // Verify logs have correct IDs
        SequentialLog firstLog = repository.findById(101);
        assertNotNull(firstLog);
        assertEquals("Range log 0", firstLog.getMessage());

        SequentialLog lastLog = repository.findById(150);
        assertNotNull(lastLog);
        assertEquals("Range log 49", lastLog.getMessage());
        System.out.println("✓ Verified range IDs correctly assigned");
    }

    @Test
    void testInsertWithinCustomRange() throws SQLException {
        System.out.println("\n=== Test 7: Insert Within Custom Range ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .allowClientIds(true)
            .retentionDays(7)
            .build();

        // Create logs
        List<SequentialLog> logs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SequentialLog log = new SequentialLog();
            log.setTimestamp(LocalDateTime.now());
            log.setMessage("Custom range " + i);
            log.setLevel("DEBUG");
            logs.add(log);
        }

        // Insert with custom range 500-509
        repository.insertWithinIdRange(logs, 500, 509);
        System.out.println("✓ Inserted with custom range 500-509");

        // Verify counter updated
        assertEquals(509, repository.getCurrentId());

        // Verify logs have correct IDs
        for (int i = 0; i < 10; i++) {
            SequentialLog log = repository.findById(500 + i);
            assertNotNull(log);
            assertEquals("Custom range " + i, log.getMessage());
        }
        System.out.println("✓ All logs have correct IDs in custom range");

        // Test error cases
        List<SequentialLog> tooManyLogs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            SequentialLog log = new SequentialLog();
            log.setTimestamp(LocalDateTime.now());
            log.setMessage("Too many");
            tooManyLogs.add(log);
        }

        assertThrows(IllegalArgumentException.class,
            () -> repository.insertWithinIdRange(tooManyLogs, 600, 610),
            "Should fail when too many entities for range");
        System.out.println("✓ Correctly rejected too many entities for range");
    }

    @Test
    void testStatePersistence() throws SQLException {
        System.out.println("\n=== Test 8: State Persistence ===");

        // First repository instance
        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .retentionDays(7)
            .build();

        // Generate some IDs
        repository.getNextN(100);
        long lastId = repository.getCurrentId();
        assertEquals(100, lastId);
        System.out.println("✓ Generated 100 IDs, current: " + lastId);

        // Shutdown first instance
        repository.shutdown();

        // Create new instance - should continue from where we left off
        SimpleSequentialRepository<SequentialLog> newRepo =
            SimpleSequentialRepository.builder(SequentialLog.class)
                .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
                .tableName("sequential_logs")
                .maxId(10000)
                .retentionDays(7)
                .build();

        long nextId = newRepo.getNextId();
        assertEquals(101, nextId, "Should continue from persisted state");
        System.out.println("✓ New instance continued from ID: " + nextId);

        newRepo.shutdown();
    }

    @Test
    void testResetId() throws SQLException {
        System.out.println("\n=== Test 9: Reset ID ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .retentionDays(7)
            .build();

        // Generate some IDs
        repository.getNextN(50);
        assertEquals(50, repository.getCurrentId());
        System.out.println("✓ Generated 50 IDs");

        // Reset to specific value
        repository.resetId(1000);
        assertEquals(1000, repository.getCurrentId());
        System.out.println("✓ Reset ID to 1000");

        // Next ID should be 1001
        long nextId = repository.getNextId();
        assertEquals(1001, nextId);
        System.out.println("✓ Next ID after reset: " + nextId);

        // Test invalid reset values
        assertThrows(IllegalArgumentException.class, () -> repository.resetId(-1));
        assertThrows(IllegalArgumentException.class, () -> repository.resetId(10001));
        System.out.println("✓ Invalid reset values correctly rejected");
    }

    @Test
    void testConcurrentIdGeneration() throws Exception {
        System.out.println("\n=== Test 10: Concurrent ID Generation ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(100000)
            .retentionDays(7)
            .build();

        // Test concurrent ID generation from multiple threads
        int threadCount = 10;
        int idsPerThread = 100;
        Set<Long> allIds = Collections.synchronizedSet(new HashSet<>());
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < idsPerThread; j++) {
                    long id = repository.getNextId();
                    allIds.add(id);
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify no duplicate IDs
        assertEquals(threadCount * idsPerThread, allIds.size(),
            "Should have unique IDs from all threads");
        System.out.println("✓ Generated " + allIds.size() + " unique IDs from " +
            threadCount + " concurrent threads");

        // Verify sequential nature (all IDs from 1 to total)
        for (int i = 1; i <= threadCount * idsPerThread; i++) {
            assertTrue(allIds.contains((long) i), "Should contain ID: " + i);
        }
        System.out.println("✓ All IDs are sequential from 1 to " + (threadCount * idsPerThread));
    }

    @Test
    void testDateRangeQueries() throws SQLException {
        System.out.println("\n=== Test 11: Date Range Queries ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .retentionDays(7)
            .build();

        // Insert logs across multiple days
        List<SequentialLog> logs = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        for (int daysAgo = 0; daysAgo < 3; daysAgo++) {
            for (int i = 0; i < 10; i++) {
                SequentialLog log = new SequentialLog();
                log.setTimestamp(now.minusDays(daysAgo).plusHours(i));
                log.setMessage("Day " + daysAgo + " log " + i);
                log.setLevel("INFO");
                logs.add(log);
            }
        }

        List<Long> ids = repository.insertMultipleWithAutoId(logs);
        System.out.println("✓ Inserted 30 logs across 3 days with IDs: " +
            ids.get(0) + "-" + ids.get(ids.size()-1));

        // Query logs from yesterday
        LocalDateTime yesterday = now.minusDays(1);
        LocalDateTime startOfYesterday = yesterday.withHour(0).withMinute(0).withSecond(0);
        LocalDateTime endOfYesterday = yesterday.withHour(23).withMinute(59).withSecond(59);

        List<SequentialLog> yesterdayLogs = repository.getDelegate().findAllByPartitionRange(
            startOfYesterday, endOfYesterday);
        System.out.println("✓ Found " + yesterdayLogs.size() + " logs from yesterday");

        // Test sequential retrieval
        String cursor = null;
        List<SequentialLog> allRetrieved = new ArrayList<>();

        while (true) {
            List<SequentialLog> batch = repository.getDelegate().findBatchByIdGreaterThan(cursor, 10);
            if (batch.isEmpty()) break;
            allRetrieved.addAll(batch);
            cursor = batch.get(batch.size() - 1).getId();
        }

        assertEquals(30, allRetrieved.size());
        System.out.println("✓ Sequential retrieval found all " + allRetrieved.size() + " logs");
    }

    @Test
    void testMixedOperations() throws SQLException {
        System.out.println("\n=== Test 12: Mixed Operations ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .allowClientIds(true)
            .retentionDays(7)
            .build();

        // Mix of auto-generated and client IDs
        long autoId1 = repository.getNextId();  // 1
        assertEquals(1, autoId1);

        repository.insertWithClientId(createLog("Client 100"), 100);
        assertEquals(100, repository.getCurrentId());

        long autoId2 = repository.getNextId();  // 101
        assertEquals(101, autoId2);

        // Reserve range
        SimpleSequentialRepository.IdRange range = repository.reserveIdRange(50);  // 102-151
        assertEquals(102, range.getStart());

        // Client ID in middle
        repository.insertWithClientId(createLog("Client 200"), 200);
        assertEquals(200, repository.getCurrentId());

        // Continue auto generation
        long autoId3 = repository.getNextId();  // 201
        assertEquals(201, autoId3);

        System.out.println("✓ Mixed operations sequence: auto(1) -> client(100) -> auto(101) -> " +
            "range(102-151) -> client(200) -> auto(201)");

        // Verify all operations maintained consistency
        assertNotNull(repository.findById(1));
        assertNotNull(repository.findById(100));
        assertNotNull(repository.findById(101));
        assertNotNull(repository.findById(200));
        assertNotNull(repository.findById(201));
        System.out.println("✓ All IDs retrievable and consistent");
    }

    @Test
    void testErrorHandling() throws SQLException {
        System.out.println("\n=== Test 13: Error Handling ===");

        // Test client IDs when not allowed
        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(10000)
            .allowClientIds(false)  // Disabled
            .retentionDays(7)
            .build();

        assertThrows(IllegalStateException.class,
            () -> repository.insertWithClientId(createLog("test"), 100),
            "Should throw when client IDs not allowed");
        System.out.println("✓ Client ID rejected when not allowed");

        // Test invalid ID values
        SimpleSequentialRepository<SequentialLog> clientRepo =
            SimpleSequentialRepository.builder(SequentialLog.class)
                .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
                .tableName("sequential_logs2")
                .maxId(1000)
                .allowClientIds(true)
                .build();

        assertThrows(IllegalArgumentException.class,
            () -> clientRepo.insertWithClientId(createLog("test"), -1),
            "Should reject negative ID");

        assertThrows(IllegalArgumentException.class,
            () -> clientRepo.insertWithClientId(createLog("test"), 1001),
            "Should reject ID > maxId");
        System.out.println("✓ Invalid ID values correctly rejected");

        // Test invalid range requests
        assertThrows(IllegalArgumentException.class,
            () -> clientRepo.reserveIdRange(-1),
            "Should reject negative count");

        assertThrows(IllegalArgumentException.class,
            () -> clientRepo.reserveIdRange(2000),
            "Should reject count > maxId");
        System.out.println("✓ Invalid range requests rejected");

        clientRepo.shutdown();
    }

    @Test
    void testLargeScaleOperations() throws SQLException {
        System.out.println("\n=== Test 14: Large Scale Operations ===");

        repository = SimpleSequentialRepository.builder(SequentialLog.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("sequential_logs")
            .maxId(Long.MAX_VALUE)
            .retentionDays(7)
            .build();

        // Generate large batch
        int batchSize = 10000;
        List<Long> ids = repository.getNextN(batchSize);
        assertEquals(batchSize, ids.size());
        assertEquals(1, ids.get(0));
        assertEquals(batchSize, ids.get(ids.size()-1));
        System.out.println("✓ Generated " + batchSize + " IDs efficiently");

        // Insert large batch
        List<SequentialLog> logs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            logs.add(createLog("Batch log " + i));
        }

        long startTime = System.currentTimeMillis();
        List<Long> assignedIds = repository.insertMultipleWithAutoId(logs);
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(1000, assignedIds.size());
        System.out.println("✓ Inserted 1000 logs in " + duration + "ms");

        // Verify sequential retrieval handles large datasets
        String cursor = null;
        int totalRetrieved = 0;

        while (totalRetrieved < 100) {  // Just retrieve first 100 for test
            List<SequentialLog> batch = repository.getDelegate().findBatchByIdGreaterThan(cursor, 50);
            if (batch.isEmpty()) break;
            totalRetrieved += batch.size();
            cursor = batch.get(batch.size() - 1).getId();
        }

        assertTrue(totalRetrieved >= 100);
        System.out.println("✓ Sequential retrieval works with large datasets");
    }

    // Helper method to create a log
    private SequentialLog createLog(String message) {
        SequentialLog log = new SequentialLog();
        log.setTimestamp(LocalDateTime.now());
        log.setMessage(message);
        log.setLevel("INFO");
        return log;
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (repository != null) {
            repository.shutdown();
        }

        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            }
            connection.close();
        }
    }

    /**
     * Sequential Log entity for testing
     */
    @Table(name = "sequential_logs")
    public static class SequentialLog implements ShardingEntity<LocalDateTime> {

        @Id(autoGenerated = false)  // We generate IDs externally
        @Column(name = "id")
        private String id;  // Will store numeric ID as string

        @com.telcobright.core.annotation.ShardingKey
        @Column(name = "timestamp")
        private LocalDateTime timestamp;

        @Column(name = "message")
        private String message;

        @Column(name = "level")
        private String level;

        @Override
        public String getId() {
            return id;
        }

        @Override
        public void setId(String id) {
            this.id = id;
        }

        @Override
        public void setPartitionColValue(LocalDateTime value) {
            this.timestamp = value;
        }

        @Override
        public LocalDateTime getPartitionColValue() {
            return timestamp;
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }
    }
}