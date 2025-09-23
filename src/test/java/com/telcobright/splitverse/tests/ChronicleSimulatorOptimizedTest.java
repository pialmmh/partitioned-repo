package com.telcobright.splitverse.tests;

import com.telcobright.core.annotation.*;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.SimpleSequentialRepository;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Optimized Chronicle Sequential Write Test
 * Tests writing and retrieving 100,000 records with client-managed sequential IDs
 */
public class ChronicleSimulatorOptimizedTest {

    private static final String TEST_DB = "chronicle_opt_db";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    private static final int TOTAL_RECORDS = 100_000;
    private static final long STARTING_SEQUENCE = 1_000_000L;

    private SimpleSequentialRepository<Event> repository;
    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false", HOST, PORT);
        connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            stmt.execute("CREATE DATABASE " + TEST_DB);
        }

        repository = SimpleSequentialRepository.builder(Event.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("events")
            .maxId(Long.MAX_VALUE)
            .allowClientIds(true)
            .retentionDays(7)
            .build();

        System.out.println("\n=== Chronicle Optimized Test ===");
        System.out.println("Database: " + TEST_DB);
        System.out.println("Target Records: " + TOTAL_RECORDS);
        System.out.println("================================\n");
    }

    @Test
    @DisplayName("Write 100K Records and Verify Retrieval")
    void testWriteAndRetrieve() throws SQLException {
        // PHASE 1: Write Records
        System.out.println("PHASE 1: Writing " + TOTAL_RECORDS + " records...");
        long writeStart = System.currentTimeMillis();

        // Batch write for better performance
        int batchSize = 1000;
        List<Event> batch = new ArrayList<>(batchSize);

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            long sequence = STARTING_SEQUENCE + i;
            Event event = new Event();
            event.setSequence(sequence);
            event.setTimestamp(LocalDateTime.now());
            event.setData("Event-" + sequence);

            repository.insertWithClientId(event, sequence);

            // Progress report
            if ((i + 1) % 10000 == 0) {
                System.out.println("  Progress: " + (i + 1) + "/" + TOTAL_RECORDS);
            }
        }

        long writeDuration = System.currentTimeMillis() - writeStart;
        double writeRate = (TOTAL_RECORDS * 1000.0) / writeDuration;
        System.out.println("✓ Write completed in " + writeDuration + "ms");
        System.out.println("  Write rate: " + String.format("%.0f", writeRate) + " records/sec\n");

        // PHASE 2: Verify Count
        System.out.println("PHASE 2: Verifying record count...");
        String countQuery = "SELECT COUNT(*) FROM " + TEST_DB + ".events";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            rs.next();
            long count = rs.getLong(1);
            assertEquals(TOTAL_RECORDS, count);
            System.out.println("✓ Record count verified: " + count + "\n");
        }

        // PHASE 3: Test Retrieval with Various Starting Points
        System.out.println("PHASE 3: Testing retrieval from different starting points...");

        // Test 1: From beginning
        testRetrieval(STARTING_SEQUENCE, 1000, "from beginning");

        // Test 2: From middle
        testRetrieval(STARTING_SEQUENCE + 50000, 1000, "from middle");

        // Test 3: From near end
        testRetrieval(STARTING_SEQUENCE + 99000, 1000, "from near end");

        // Test 4: Random starting points
        System.out.println("\nTest: Random starting points");
        Random random = new Random();
        long totalReadTime = 0;
        int readTests = 10;

        for (int i = 0; i < readTests; i++) {
            long startSeq = STARTING_SEQUENCE + random.nextInt(TOTAL_RECORDS - 1000);
            long readStart = System.currentTimeMillis();

            List<Event> events = retrieveFromSequence(startSeq, 1000);

            long readTime = System.currentTimeMillis() - readStart;
            totalReadTime += readTime;

            assertEquals(1000, events.size());
            verifySequence(events, startSeq);
        }

        double avgReadTime = totalReadTime / (double) readTests;
        System.out.println("✓ Average read time for 1000 records: " +
                          String.format("%.1f", avgReadTime) + "ms");

        // PHASE 4: Verify Sequence Integrity
        System.out.println("\nPHASE 4: Verifying sequence integrity...");

        // Check min/max sequences
        String rangeQuery = "SELECT MIN(CAST(id AS UNSIGNED)), MAX(CAST(id AS UNSIGNED)) FROM " + TEST_DB + ".events";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(rangeQuery)) {
            rs.next();
            long minSeq = rs.getLong(1);
            long maxSeq = rs.getLong(2);
            assertEquals(STARTING_SEQUENCE, minSeq);
            assertEquals(STARTING_SEQUENCE + TOTAL_RECORDS - 1, maxSeq);
            System.out.println("✓ Sequence range verified: " + minSeq + " to " + maxSeq);
        }

        System.out.println("\n=== TEST COMPLETED SUCCESSFULLY ===");
        System.out.println("✓ All " + TOTAL_RECORDS + " records written and verified");
    }

    private void testRetrieval(long startSeq, int count, String description) {
        System.out.println("\nTest: Retrieve " + count + " records " + description);
        long start = System.currentTimeMillis();

        List<Event> events = retrieveFromSequence(startSeq, count);

        long duration = System.currentTimeMillis() - start;

        assertEquals(count, events.size(), "Should retrieve exact count");
        verifySequence(events, startSeq);

        System.out.println("✓ Retrieved " + events.size() + " records in " + duration + "ms");
        System.out.println("  Sequence range: " + events.get(0).getSequence() +
                          " to " + events.get(events.size()-1).getSequence());
    }

    private List<Event> retrieveFromSequence(long startSeq, int count) {
        try {
            // Simple batch retrieval: startId + batchSize
            return repository.findByIdRange(startSeq, count);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve events", e);
        }
    }

    private void verifySequence(List<Event> events, long expectedStart) {
        long expectedSeq = expectedStart;
        for (Event event : events) {
            assertEquals(expectedSeq, event.getSequence(),
                        "Sequence mismatch at position " + (expectedSeq - expectedStart));
            expectedSeq++;
        }
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

    @Table(name = "events")
    public static class Event implements ShardingEntity<LocalDateTime> {

        @Id(autoGenerated = false)
        @Column(name = "id")
        private String id;

        @Column(name = "sequence")
        private long sequence;

        @com.telcobright.core.annotation.ShardingKey
        @Column(name = "timestamp")
        private LocalDateTime timestamp;

        @Column(name = "data")
        private String data;

        @Override
        public String getId() {
            return id;
        }

        @Override
        public void setId(String id) {
            this.id = id;
            try {
                this.sequence = Long.parseLong(id);
            } catch (NumberFormatException e) {
                // Handle non-numeric IDs
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

        public long getSequence() {
            return sequence;
        }

        public void setSequence(long sequence) {
            this.sequence = sequence;
            this.id = String.valueOf(sequence);
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }
}