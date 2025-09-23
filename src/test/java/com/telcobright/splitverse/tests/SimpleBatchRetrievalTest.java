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
 * Test for simplified batch retrieval using findByIdRange
 */
public class SimpleBatchRetrievalTest {

    private static final String TEST_DB = "batch_test_db";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    private SimpleSequentialRepository<Record> repository;
    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false", HOST, PORT);
        connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            stmt.execute("CREATE DATABASE " + TEST_DB);
        }

        repository = SimpleSequentialRepository.builder(Record.class)
            .connection(HOST, PORT, TEST_DB, USERNAME, PASSWORD)
            .tableName("records")
            .allowClientIds(true)
            .build();
    }

    @Test
    @DisplayName("Test simple batch retrieval with findByIdRange")
    void testSimpleBatchRetrieval() throws SQLException {
        System.out.println("\n=== Simple Batch Retrieval Test ===\n");

        // PHASE 1: Insert test data with known IDs
        System.out.println("PHASE 1: Inserting test records...");

        // Insert records with IDs 1-100, 1000-1099, 5000-5049
        insertRecordRange(1, 100);
        insertRecordRange(1000, 100);
        insertRecordRange(5000, 50);

        System.out.println("✓ Inserted 250 test records\n");

        // PHASE 2: Test batch retrieval scenarios
        System.out.println("PHASE 2: Testing batch retrieval scenarios...\n");

        // Test 1: Retrieve first 100 records (1-100)
        System.out.println("Test 1: findByIdRange(1, 100) - expect IDs 1-100");
        List<Record> batch1 = repository.findByIdRange(1, 100);
        assertEquals(100, batch1.size());
        assertEquals(1L, batch1.get(0).getNumericId());
        assertEquals(100L, batch1.get(99).getNumericId());
        System.out.println("✓ Retrieved " + batch1.size() + " records (IDs " +
                          batch1.get(0).getNumericId() + "-" + batch1.get(batch1.size()-1).getNumericId() + ")\n");

        // Test 2: Retrieve records 1000-1099
        System.out.println("Test 2: findByIdRange(1000, 100) - expect IDs 1000-1099");
        List<Record> batch2 = repository.findByIdRange(1000, 100);
        assertEquals(100, batch2.size());
        assertEquals(1000L, batch2.get(0).getNumericId());
        assertEquals(1099L, batch2.get(99).getNumericId());
        System.out.println("✓ Retrieved " + batch2.size() + " records (IDs " +
                          batch2.get(0).getNumericId() + "-" + batch2.get(batch2.size()-1).getNumericId() + ")\n");

        // Test 3: Retrieve with gaps (101-200, no data exists)
        System.out.println("Test 3: findByIdRange(101, 100) - expect empty (no data in range)");
        List<Record> batch3 = repository.findByIdRange(101, 100);
        assertEquals(0, batch3.size());
        System.out.println("✓ Retrieved " + batch3.size() + " records (range has no data)\n");

        // Test 4: Partial range (5000-5099, only 50 exist)
        System.out.println("Test 4: findByIdRange(5000, 100) - expect 50 records (5000-5049)");
        List<Record> batch4 = repository.findByIdRange(5000, 100);
        assertEquals(50, batch4.size());
        assertEquals(5000L, batch4.get(0).getNumericId());
        assertEquals(5049L, batch4.get(49).getNumericId());
        System.out.println("✓ Retrieved " + batch4.size() + " records (IDs " +
                          batch4.get(0).getNumericId() + "-" + batch4.get(batch4.size()-1).getNumericId() + ")\n");

        // Test 5: Small batch size
        System.out.println("Test 5: findByIdRange(1000, 10) - expect IDs 1000-1009");
        List<Record> batch5 = repository.findByIdRange(1000, 10);
        assertEquals(10, batch5.size());
        assertEquals(1000L, batch5.get(0).getNumericId());
        assertEquals(1009L, batch5.get(9).getNumericId());
        System.out.println("✓ Retrieved " + batch5.size() + " records (IDs " +
                          batch5.get(0).getNumericId() + "-" + batch5.get(batch5.size()-1).getNumericId() + ")\n");

        // PHASE 3: Test sequential iteration pattern
        System.out.println("PHASE 3: Testing sequential iteration pattern...\n");

        int totalFound = 0;
        long currentId = 1;
        int batchSize = 500;

        while (currentId <= 6000) {
            List<Record> batch = repository.findByIdRange(currentId, batchSize);
            totalFound += batch.size();

            if (!batch.isEmpty()) {
                System.out.println("Batch starting at " + currentId + ": found " + batch.size() + " records");
            }

            currentId += batchSize;
        }

        assertEquals(250, totalFound);
        System.out.println("\n✓ Sequential iteration found all " + totalFound + " records\n");

        // PHASE 4: Performance test
        System.out.println("PHASE 4: Performance test...\n");

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            repository.findByIdRange(i * 100, 100);
        }
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("✓ 100 batch retrievals completed in " + duration + "ms");
        System.out.println("  Average: " + (duration / 100.0) + "ms per batch\n");

        System.out.println("=== TEST COMPLETED SUCCESSFULLY ===");
    }

    private void insertRecordRange(long startId, int count) throws SQLException {
        List<Record> records = new ArrayList<>();
        for (long id = startId; id < startId + count; id++) {
            Record record = new Record();
            record.setId(id);
            record.setTimestamp(LocalDateTime.now());
            record.setData("Record-" + id);
            records.add(record);
        }

        // Create list of IDs for batch insert
        List<Long> ids = new ArrayList<>();
        for (Record record : records) {
            ids.add(record.getNumericId());
        }

        repository.insertMultipleWithClientIds(records, ids);
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

    @Table(name = "records")
    public static class Record implements ShardingEntity<LocalDateTime> {

        @Id(autoGenerated = false)
        @Column(name = "id")
        private String id;

        private long numericId;

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
                this.numericId = Long.parseLong(id);
            } catch (NumberFormatException e) {
                // Handle non-numeric IDs
            }
        }

        public void setId(long id) {
            this.numericId = id;
            this.id = String.valueOf(id);
        }

        public long getNumericId() {
            return numericId;
        }

        @Override
        public LocalDateTime getPartitionColValue() {
            return timestamp;
        }

        @Override
        public void setPartitionColValue(LocalDateTime value) {
            this.timestamp = value;
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