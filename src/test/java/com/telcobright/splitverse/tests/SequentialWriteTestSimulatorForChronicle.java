package com.telcobright.splitverse.tests;

import com.telcobright.core.annotation.*;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.SimpleSequentialRepository;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sequential Write Test Simulator for Chronicle-like Systems
 *
 * This test simulates a Chronicle Queue-like client that:
 * 1. Sends events with client-managed sequential IDs (not handled by split-verse)
 * 2. Writes 100,000 records with incremental sequence numbers
 * 3. Verifies retrieval starting from arbitrary sequence numbers with configurable batch sizes
 * 4. Validates that each record's sequence number is correct and in order
 *
 * Chronicle Queue is a high-performance persistence library that maintains strict ordering
 * of messages. This test simulates similar behavior using split-verse's SimpleSequentialRepository
 * with client-provided IDs.
 *
 * Test Scenarios:
 * - Sequential write of 100K events with client-managed IDs
 * - Random starting point retrieval with various batch sizes
 * - Sequence continuity verification
 * - Performance metrics collection
 * - Edge case handling (boundaries, gaps, etc.)
 */
public class SequentialWriteTestSimulatorForChronicle {

    private static final String TEST_DB = "chronicle_test_db";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    // Test parameters
    private static final int TOTAL_RECORDS = 100_000;
    private static final long STARTING_SEQUENCE = 1_000_000L; // Start from 1 million for clear identification

    private SimpleSequentialRepository<ChronicleEvent> repository;
    private Connection connection;
    private final AtomicLong sequenceGenerator = new AtomicLong(STARTING_SEQUENCE);

    @BeforeEach
    void setUp() throws SQLException {
        // Create test database
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false", HOST, PORT);
        connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            stmt.execute("CREATE DATABASE " + TEST_DB);
        }

        // Initialize repository with client ID support
        repository = SimpleSequentialRepository.builder(ChronicleEvent.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("chronicle_events")
            .maxId(Long.MAX_VALUE)
            .allowClientIds(true)  // Important: Allow client-managed IDs
            .retentionDays(30)
            .build();

        System.out.println("\n=== Chronicle Sequential Write Test Simulator ===");
        System.out.println("Database: " + TEST_DB);
        System.out.println("Total Records: " + TOTAL_RECORDS);
        System.out.println("Starting Sequence: " + STARTING_SEQUENCE);
        System.out.println("================================================\n");
    }

    @Test
    @DisplayName("Sequential Write and Retrieval with 100K Records")
    void testSequentialWriteAndRetrieval() throws SQLException {
        // Phase 1: Write 100K records with client-managed sequential IDs
        System.out.println("PHASE 1: Writing " + TOTAL_RECORDS + " sequential events...");
        long writeStartTime = System.currentTimeMillis();

        List<ChronicleEvent> writtenEvents = writeSequentialEvents(TOTAL_RECORDS);

        long writeEndTime = System.currentTimeMillis();
        long writeDuration = writeEndTime - writeStartTime;

        System.out.println("✓ Write completed in " + writeDuration + "ms");
        System.out.println("  - Records written: " + writtenEvents.size());
        System.out.println("  - Write rate: " + (TOTAL_RECORDS * 1000L / writeDuration) + " records/sec");
        System.out.println("  - Sequence range: " + STARTING_SEQUENCE + " to " +
                          (STARTING_SEQUENCE + TOTAL_RECORDS - 1));

        // Phase 2: Verify retrieval with various starting points and batch sizes
        System.out.println("\nPHASE 2: Verifying retrieval with arbitrary starting points...");

        // Test Case 1: Retrieve from beginning
        verifyRetrieval(STARTING_SEQUENCE, 1000, 1000, "from beginning");

        // Test Case 2: Retrieve from middle
        long middleSequence = STARTING_SEQUENCE + (TOTAL_RECORDS / 2);
        verifyRetrieval(middleSequence, 500, 500, "from middle");

        // Test Case 3: Retrieve from near end
        long nearEndSequence = STARTING_SEQUENCE + TOTAL_RECORDS - 100;
        verifyRetrieval(nearEndSequence, 100, 100, "from near end");

        // Test Case 4: Large batch from arbitrary point
        long arbitrarySequence = STARTING_SEQUENCE + 25000;
        verifyRetrieval(arbitrarySequence, 10000, 10000, "large batch from arbitrary point");

        // Test Case 5: Small batches with pagination
        System.out.println("\nTest Case 5: Pagination with small batches");
        verifyPagination(STARTING_SEQUENCE + 5000, 100, 10, "small batch pagination");

        // Phase 3: Performance test - rapid sequential reads
        System.out.println("\nPHASE 3: Performance test - rapid sequential reads");
        performanceTestSequentialReads();

        // Phase 4: Verify data integrity
        System.out.println("\nPHASE 4: Data integrity verification");
        verifyDataIntegrity();
    }

    /**
     * Write sequential events with client-managed IDs
     */
    private List<ChronicleEvent> writeSequentialEvents(int count) throws SQLException {
        List<ChronicleEvent> events = new ArrayList<>();
        int batchSize = 1000;

        for (int i = 0; i < count; i++) {
            long sequenceNumber = sequenceGenerator.getAndIncrement();
            ChronicleEvent event = createEvent(sequenceNumber, i);
            events.add(event);

            // Use client-provided ID
            repository.insertWithClientId(event, sequenceNumber);

            // Progress reporting
            if ((i + 1) % 10000 == 0) {
                System.out.println("  Progress: " + (i + 1) + "/" + count + " events written");
            }
        }

        return events;
    }

    /**
     * Create a Chronicle event with given sequence number
     */
    private ChronicleEvent createEvent(long sequenceNumber, int index) {
        ChronicleEvent event = new ChronicleEvent();
        event.setSequenceNumber(sequenceNumber);
        event.setTimestamp(LocalDateTime.now().plusNanos(index * 1000)); // Ensure unique timestamps
        event.setEventType("MARKET_DATA");
        event.setPayload(generatePayload(sequenceNumber));
        event.setChecksum(calculateChecksum(sequenceNumber));
        return event;
    }

    /**
     * Generate realistic payload for the event
     */
    private String generatePayload(long sequenceNumber) {
        return String.format(
            "{\"seq\":%d,\"symbol\":\"EUR/USD\",\"bid\":%.5f,\"ask\":%.5f,\"volume\":%d,\"timestamp\":%d}",
            sequenceNumber,
            1.1000 + (sequenceNumber % 1000) * 0.00001,
            1.1002 + (sequenceNumber % 1000) * 0.00001,
            1000 + (sequenceNumber % 10000),
            System.currentTimeMillis()
        );
    }

    /**
     * Calculate checksum for data integrity
     */
    private long calculateChecksum(long sequenceNumber) {
        // Simple checksum for demonstration
        return sequenceNumber * 31 + 17;
    }

    /**
     * Verify retrieval starting from arbitrary sequence number with specified batch size
     */
    private void verifyRetrieval(long startSequence, int batchSize, int expectedCount, String testCase) {
        System.out.println("\nTest Case: " + testCase);
        System.out.println("  Starting sequence: " + startSequence);
        System.out.println("  Batch size: " + batchSize);

        List<ChronicleEvent> retrieved = retrieveEventsFromSequence(startSequence, batchSize);

        // Verify count
        assertEquals(expectedCount, retrieved.size(),
            "Expected " + expectedCount + " records but got " + retrieved.size());
        System.out.println("  ✓ Retrieved correct count: " + retrieved.size());

        // Verify sequence continuity
        long expectedSeq = startSequence;
        for (ChronicleEvent event : retrieved) {
            assertEquals(expectedSeq, event.getSequenceNumber(),
                "Sequence mismatch: expected " + expectedSeq + " but got " + event.getSequenceNumber());

            // Verify checksum
            long expectedChecksum = calculateChecksum(event.getSequenceNumber());
            assertEquals(expectedChecksum, event.getChecksum(),
                "Checksum mismatch for sequence " + event.getSequenceNumber());

            expectedSeq++;
        }
        System.out.println("  ✓ Sequence continuity verified");
        System.out.println("  ✓ Checksums verified");
    }

    /**
     * Retrieve events starting from a specific sequence number
     */
    private List<ChronicleEvent> retrieveEventsFromSequence(long startSequence, int batchSize) {
        List<ChronicleEvent> result = new ArrayList<>();

        try {
            // Since IDs are stored as strings but represent numbers, we need proper ordering
            // Use direct repository method with the sequence number as string
            ChronicleEvent startEvent = repository.findById(startSequence);

            // If we can't find the exact start, try to find records greater than or equal
            if (startEvent != null) {
                result.add(startEvent);
            }

            // Get remaining records if needed
            if (result.size() < batchSize) {
                String cursor = String.valueOf(startSequence);
                int remaining = batchSize - result.size();

                List<ChronicleEvent> batch = repository.getDelegate()
                    .findBatchByIdGreaterThan(cursor, remaining);

                for (ChronicleEvent event : batch) {
                    if (result.size() < batchSize) {
                        result.add(event);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve events from sequence " + startSequence, e);
        }

        return result;
    }

    /**
     * Verify pagination with multiple small batches
     */
    private void verifyPagination(long startSequence, int totalRecords, int pageSize, String testCase) {
        System.out.println("  Starting sequence: " + startSequence);
        System.out.println("  Total records: " + totalRecords);
        System.out.println("  Page size: " + pageSize);

        List<ChronicleEvent> allRetrieved = new ArrayList<>();
        long currentSequence = startSequence;
        int pages = 0;

        while (allRetrieved.size() < totalRecords) {
            List<ChronicleEvent> page = retrieveEventsFromSequence(currentSequence, pageSize);
            if (page.isEmpty()) {
                break;
            }

            allRetrieved.addAll(page);
            currentSequence = page.get(page.size() - 1).getSequenceNumber() + 1;
            pages++;
        }

        assertEquals(totalRecords, allRetrieved.size(),
            "Expected " + totalRecords + " total records but got " + allRetrieved.size());
        System.out.println("  ✓ Retrieved " + allRetrieved.size() + " records in " + pages + " pages");

        // Verify sequence continuity across pages
        long expectedSeq = startSequence;
        for (ChronicleEvent event : allRetrieved) {
            assertEquals(expectedSeq++, event.getSequenceNumber());
        }
        System.out.println("  ✓ Sequence continuity verified across all pages");
    }

    /**
     * Performance test for sequential reads
     */
    private void performanceTestSequentialReads() {
        int testIterations = 10;
        int recordsPerIteration = 1000;
        long totalReadTime = 0;

        for (int i = 0; i < testIterations; i++) {
            // Random starting point
            long startSeq = STARTING_SEQUENCE + ThreadLocalRandom.current().nextInt(TOTAL_RECORDS - recordsPerIteration);

            long startTime = System.nanoTime();
            List<ChronicleEvent> events = retrieveEventsFromSequence(startSeq, recordsPerIteration);
            long readTime = System.nanoTime() - startTime;

            totalReadTime += readTime;

            assertEquals(recordsPerIteration, events.size());
        }

        double avgReadTimeMs = totalReadTime / (testIterations * 1_000_000.0);
        double readsPerSecond = (testIterations * recordsPerIteration * 1_000_000_000.0) / totalReadTime;

        System.out.println("  Average read time for 1000 records: " + String.format("%.2f", avgReadTimeMs) + "ms");
        System.out.println("  Read throughput: " + String.format("%.0f", readsPerSecond) + " records/sec");
        System.out.println("  ✓ Performance test completed");
    }

    /**
     * Comprehensive data integrity verification
     */
    private void verifyDataIntegrity() throws SQLException {
        // Verify total count
        String countQuery = "SELECT COUNT(*) FROM " + TEST_DB + ".chronicle_events";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            rs.next();
            long totalCount = rs.getLong(1);
            assertEquals(TOTAL_RECORDS, totalCount);
            System.out.println("  ✓ Total record count verified: " + totalCount);
        }

        // Verify sequence range
        String rangeQuery = "SELECT MIN(CAST(id AS UNSIGNED)), MAX(CAST(id AS UNSIGNED)) FROM " +
                           TEST_DB + ".chronicle_events";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(rangeQuery)) {
            rs.next();
            long minSeq = rs.getLong(1);
            long maxSeq = rs.getLong(2);
            assertEquals(STARTING_SEQUENCE, minSeq);
            assertEquals(STARTING_SEQUENCE + TOTAL_RECORDS - 1, maxSeq);
            System.out.println("  ✓ Sequence range verified: " + minSeq + " to " + maxSeq);
        }

        // Sample random records for detailed verification
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            long randomSeq = STARTING_SEQUENCE + random.nextInt(TOTAL_RECORDS);
            ChronicleEvent event = repository.findById(randomSeq);

            assertNotNull(event);
            assertEquals(randomSeq, event.getSequenceNumber());
            assertEquals(calculateChecksum(randomSeq), event.getChecksum());
        }
        System.out.println("  ✓ Random sample verification passed (100 records)");
    }

    @Test
    @DisplayName("Boundary and Edge Case Testing")
    void testBoundaryConditions() throws SQLException {
        // Write a smaller set for edge case testing
        int testSize = 1000;
        writeSequentialEvents(testSize);

        // Test 1: Retrieve exactly at boundaries
        System.out.println("\nBoundary Test 1: Exact boundary retrieval");
        verifyRetrieval(STARTING_SEQUENCE, 1, 1, "first record only");
        verifyRetrieval(STARTING_SEQUENCE + testSize - 1, 1, 1, "last record only");

        // Test 2: Request more than available
        System.out.println("\nBoundary Test 2: Request exceeding available");
        List<ChronicleEvent> overRequest = retrieveEventsFromSequence(
            STARTING_SEQUENCE + testSize - 10, 20);
        assertEquals(10, overRequest.size(), "Should only get 10 available records");
        System.out.println("  ✓ Correctly handled over-request");

        // Test 3: Request from beyond range
        System.out.println("\nBoundary Test 3: Request from beyond range");
        List<ChronicleEvent> beyondRange = retrieveEventsFromSequence(
            STARTING_SEQUENCE + testSize + 100, 10);
        assertTrue(beyondRange.isEmpty(), "Should get no records from beyond range");
        System.out.println("  ✓ Correctly handled beyond-range request");

        // Test 4: Zero and negative batch sizes (should handle gracefully)
        System.out.println("\nBoundary Test 4: Invalid batch sizes");
        List<ChronicleEvent> zeroBatch = retrieveEventsFromSequence(STARTING_SEQUENCE, 0);
        assertTrue(zeroBatch.isEmpty(), "Zero batch should return empty");
        System.out.println("  ✓ Zero batch size handled correctly");
    }

    @Test
    @DisplayName("Concurrent Write and Read Operations")
    void testConcurrentOperations() throws Exception {
        System.out.println("\n=== Concurrent Operations Test ===");

        // Start with some initial data
        int initialRecords = 10000;
        writeSequentialEvents(initialRecords);

        // Create concurrent writer thread
        Thread writerThread = new Thread(() -> {
            try {
                for (int i = 0; i < 5000; i++) {
                    long seq = sequenceGenerator.getAndIncrement();
                    ChronicleEvent event = createEvent(seq, i);
                    repository.insertWithClientId(event, seq);

                    if (i % 1000 == 0) {
                        System.out.println("  Writer: " + i + " records written");
                    }
                    Thread.sleep(1); // Simulate realistic write rate
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Create concurrent reader thread
        Thread readerThread = new Thread(() -> {
            try {
                long lastSeenSequence = STARTING_SEQUENCE + initialRecords - 1;
                int iterations = 0;

                while (iterations < 10) {
                    List<ChronicleEvent> newEvents = retrieveEventsFromSequence(
                        lastSeenSequence + 1, 100);

                    if (!newEvents.isEmpty()) {
                        lastSeenSequence = newEvents.get(newEvents.size() - 1).getSequenceNumber();
                        System.out.println("  Reader: Read " + newEvents.size() +
                                         " new events, last seq: " + lastSeenSequence);
                    }

                    Thread.sleep(500); // Check for new events every 500ms
                    iterations++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Run both threads
        writerThread.start();
        readerThread.start();

        // Wait for completion
        writerThread.join();
        readerThread.join();

        System.out.println("✓ Concurrent operations test completed");
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
     * Chronicle Event entity - simulates a Chronicle Queue message
     */
    @Table(name = "chronicle_events")
    public static class ChronicleEvent implements ShardingEntity<LocalDateTime> {

        @Id(autoGenerated = false)
        @Column(name = "id")
        private String id;

        @Column(name = "sequence_number")
        private long sequenceNumber;

        @com.telcobright.core.annotation.ShardingKey
        @Column(name = "timestamp")
        private LocalDateTime timestamp;

        @Column(name = "event_type")
        private String eventType;

        @Column(name = "payload")
        private String payload;

        @Column(name = "checksum")
        private long checksum;

        @Override
        public String getId() {
            return id;
        }

        @Override
        public void setId(String id) {
            this.id = id;
            // Also set sequence number from ID
            try {
                this.sequenceNumber = Long.parseLong(id);
            } catch (NumberFormatException e) {
                // Handle non-numeric IDs if needed
            }
        }

        @Override
        public LocalDateTime getPartitionColValue() {
            return timestamp;
        }

        @Override
        public void setPartitionColValue(LocalDateTime value) {
            this.timestamp = value;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public void setSequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            this.id = String.valueOf(sequenceNumber);
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public long getChecksum() {
            return checksum;
        }

        public void setChecksum(long checksum) {
            this.checksum = checksum;
        }
    }
}