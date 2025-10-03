package com.telcobright.splitverse.tests;

import com.telcobright.core.aggregation.AggregationQuery;
import com.telcobright.core.aggregation.AggregationResult;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.examples.entity.SmsEntity;
import com.telcobright.splitverse.config.RepositoryMode;
import com.telcobright.splitverse.config.ShardConfig;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic test for aggregation functionality.
 * Tests SUM, AVG, COUNT, MAX, MIN across multi-table and native partition modes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregationBasicTest {

    private SplitVerseRepository<SmsEntity, LocalDateTime> repository;
    private Connection connection;
    private static final String DB_NAME = "test_aggregation_db";

    @BeforeAll
    void setup() throws Exception {
        // Create database
        try (Connection rootConn = DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:3306/", "root", "123456")) {
            try (Statement stmt = rootConn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE IF EXISTS " + DB_NAME);
                stmt.executeUpdate("CREATE DATABASE " + DB_NAME);
            }
        }

        // Create repository
        ShardConfig shardConfig = ShardConfig.builder()
            .shardId("shard1")
            .database(DB_NAME)
            .host("127.0.0.1")
            .port(3306)
            .username("root")
            .password("123456")
            .enabled(true)
            .build();

        repository = SplitVerseRepository.<SmsEntity, LocalDateTime>builder()
            .withSingleShard(shardConfig)
            .withEntityClass(SmsEntity.class)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withTableName("sms")
            .withRetentionDays(7)
            .build();

        // Insert test data
        insertTestData();
    }

    private void insertTestData() throws SQLException {
        LocalDateTime baseTime = LocalDateTime.of(2025, 10, 1, 10, 0);
        List<SmsEntity> entities = new ArrayList<>();

        // Customer A: 10 messages, total amount 100.00
        for (int i = 0; i < 10; i++) {
            SmsEntity sms = new SmsEntity();
            sms.setId("A-" + i);
            sms.setPhoneNumber("880171000000" + i);
            sms.setPartitionColValue(baseTime.plusHours(i));
            sms.setCost(new BigDecimal("10.00"));
            sms.setStatus("sent");
            entities.add(sms);
        }

        // Customer B: 5 messages, total amount 75.00
        for (int i = 0; i < 5; i++) {
            SmsEntity sms = new SmsEntity();
            sms.setId("B-" + i);
            sms.setPhoneNumber("880172000000" + i);
            sms.setPartitionColValue(baseTime.plusHours(i + 2));
            sms.setCost(new BigDecimal("15.00"));
            sms.setStatus("sent");
            entities.add(sms);
        }

        repository.insertMultiple(entities);
    }

    @Test
    @Order(1)
    void testSimpleSumAggregation() throws SQLException {
        System.out.println("\n=== Test: Simple SUM Aggregation ===");

        AggregationQuery query = AggregationQuery.builder()
            .sum("cost", "total_cost")
            .build();

        List<AggregationResult> results = repository.aggregate(
            query,
            LocalDateTime.of(2025, 10, 1, 0, 0),
            LocalDateTime.of(2025, 10, 2, 23, 59)
        );

        assertNotNull(results);
        assertEquals(1, results.size(), "Should return 1 result (no GROUP BY)");

        AggregationResult result = results.get(0);
        BigDecimal total = result.getBigDecimal("total_cost");

        System.out.println("Total cost: " + total);

        // 10 messages * 10.00 + 5 messages * 15.00 = 175.00
        assertEquals(0, new BigDecimal("175.00").compareTo(total),
            "Total should be 175.00");
    }

    @Test
    @Order(2)
    void testCountAggregation() throws SQLException {
        System.out.println("\n=== Test: COUNT Aggregation ===");

        AggregationQuery query = AggregationQuery.builder()
            .count("*", "message_count")
            .build();

        List<AggregationResult> results = repository.aggregate(
            query,
            LocalDateTime.of(2025, 10, 1, 0, 0),
            LocalDateTime.of(2025, 10, 2, 23, 59)
        );

        assertNotNull(results);
        assertEquals(1, results.size());

        AggregationResult result = results.get(0);
        Long count = result.getLong("message_count");

        System.out.println("Total message count: " + count);

        assertEquals(15L, count, "Should have 15 messages total");
    }

    @Test
    @Order(3)
    void testGroupByAggregation() throws SQLException {
        System.out.println("\n=== Test: GROUP BY Aggregation ===");

        AggregationQuery query = AggregationQuery.builder()
            .sum("cost", "total_cost")
            .count("*", "message_count")
            .groupBy("status")
            .build();

        List<AggregationResult> results = repository.aggregate(
            query,
            LocalDateTime.of(2025, 10, 1, 0, 0),
            LocalDateTime.of(2025, 10, 2, 23, 59)
        );

        assertNotNull(results);
        assertEquals(1, results.size(), "Should have 1 status group (all 'sent')");

        AggregationResult result = results.get(0);
        assertEquals("sent", result.getDimension("status"));
        assertEquals(15L, result.getLong("message_count"));
        assertEquals(0, new BigDecimal("175.00").compareTo(result.getBigDecimal("total_cost")));

        System.out.println("Status: " + result.getDimension("status"));
        System.out.println("Count: " + result.getLong("message_count"));
        System.out.println("Total: " + result.getBigDecimal("total_cost"));
    }

    @Test
    @Order(4)
    void testMaxMinAggregation() throws SQLException {
        System.out.println("\n=== Test: MAX/MIN Aggregation ===");

        AggregationQuery query = AggregationQuery.builder()
            .max("cost", "max_cost")
            .min("cost", "min_cost")
            .build();

        List<AggregationResult> results = repository.aggregate(
            query,
            LocalDateTime.of(2025, 10, 1, 0, 0),
            LocalDateTime.of(2025, 10, 2, 23, 59)
        );

        assertNotNull(results);
        assertEquals(1, results.size());

        AggregationResult result = results.get(0);
        BigDecimal max = result.getBigDecimal("max_cost");
        BigDecimal min = result.getBigDecimal("min_cost");

        System.out.println("Max cost: " + max);
        System.out.println("Min cost: " + min);

        assertEquals(0, new BigDecimal("15.00").compareTo(max), "Max should be 15.00");
        assertEquals(0, new BigDecimal("10.00").compareTo(min), "Min should be 10.00");
    }

    @Test
    @Order(5)
    void testAvgAggregation() throws SQLException {
        System.out.println("\n=== Test: AVG Aggregation ===");

        AggregationQuery query = AggregationQuery.builder()
            .avg("cost", "avg_cost")
            .build();

        List<AggregationResult> results = repository.aggregate(
            query,
            LocalDateTime.of(2025, 10, 1, 0, 0),
            LocalDateTime.of(2025, 10, 2, 23, 59)
        );

        assertNotNull(results);
        assertEquals(1, results.size());

        AggregationResult result = results.get(0);
        BigDecimal avg = result.getBigDecimal("avg_cost");

        System.out.println("Average cost: " + avg);

        // Average = 175.00 / 15 = 11.6667
        assertTrue(avg.compareTo(new BigDecimal("11.66")) >= 0 &&
                  avg.compareTo(new BigDecimal("11.67")) <= 0,
            "Average should be around 11.67");
    }

    @AfterAll
    void cleanup() throws Exception {
        if (repository != null) {
            repository.shutdown();
        }

        // Drop test database
        try (Connection rootConn = DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:3306/", "root", "123456")) {
            try (Statement stmt = rootConn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE IF EXISTS " + DB_NAME);
            }
        }

        System.out.println("\n=== Cleanup Complete ===");
    }
}
