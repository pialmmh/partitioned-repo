package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.config.RepositoryMode;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Tests and demonstrates the repository mode selection feature
 * Shows how to configure Split-Verse for both PARTITIONED and MULTI_TABLE modes
 */
public class RepositoryModeTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) {
        System.out.println("=== Repository Mode Test ===\n");
        System.out.println("This test demonstrates how to use both repository modes through the builder:\n");
        System.out.println("1. PARTITIONED - Single table with MySQL native partitions");
        System.out.println("2. MULTI_TABLE - Separate table for each time period\n");

        try {
            // Test 1: Partitioned Mode (Default)
            testPartitionedMode();

            // Test 2: Multi-Table Mode
            testMultiTableMode();

            // Test 3: Custom Configuration
            testCustomConfiguration();

            System.out.println("\n✅ All repository mode tests passed!");

        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testPartitionedMode() throws Exception {
        System.out.println("\n1. TESTING PARTITIONED MODE");
        System.out.println("============================");

        // Setup database
        String database = "partitioned_mode_test";
        setupDatabase(database);

        // Create repository with partitioned mode (default)
        ShardConfig config = ShardConfig.builder()
            .shardId("shard1")
            .host(HOST)
            .port(PORT)
            .database(database)
            .username(USERNAME)
            .password(PASSWORD)
            .enabled(true)
            .build();

        SplitVerseRepository<SubscriberEntity, LocalDateTime> repository =
            SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                .withSingleShard(config)
                .withEntityClass(SubscriberEntity.class)
                .withPartitionType(PartitionType.DATE_BASED)
                .withRetentionDays(7)
                .withCharset("utf8mb4")
                .withCollation("utf8mb4_bin")
                .build();

        // Insert test data
        SubscriberEntity subscriber = new SubscriberEntity();
        subscriber.setId(UUID.randomUUID().toString());
        subscriber.setMsisdn("1234567890");
        // subscriber.setEmail("test@example.com"); // Email field doesn't exist
        subscriber.setCreatedAt(LocalDateTime.now());
        repository.insert(subscriber);

        // Verify table structure
        verifyPartitionedTable(database);

        repository.shutdown();
        System.out.println("✓ Partitioned mode test completed\n");
    }

    private static void testMultiTableMode() throws Exception {
        System.out.println("\n2. TESTING MULTI-TABLE MODE");
        System.out.println("============================");

        // Setup database
        String database = "multi_table_mode_test";
        setupDatabase(database);

        // Create repository with multi-table mode
        ShardConfig config = ShardConfig.builder()
            .shardId("shard1")
            .host(HOST)
            .port(PORT)
            .database(database)
            .username(USERNAME)
            .password(PASSWORD)
            .enabled(true)
            .build();

        SplitVerseRepository<SubscriberEntity, LocalDateTime> repository =
            SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                .withSingleShard(config)
                .withEntityClass(SubscriberEntity.class)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)  // Specify multi-table mode
                .withTableGranularity(GenericMultiTableRepository.TableGranularity.DAILY)
                .withRetentionDays(5)
                .withCharset("utf8mb4")
                .withCollation("utf8mb4_bin")
                .build();

        // Insert test data for different days
        for (int dayOffset = -2; dayOffset <= 2; dayOffset++) {
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(UUID.randomUUID().toString());
            subscriber.setMsisdn("123456" + dayOffset);
            // subscriber.setEmail("test" + dayOffset + "@example.com"); // Email field doesn't exist
            subscriber.setCreatedAt(LocalDateTime.now().plusDays(dayOffset));
            repository.insert(subscriber);
        }

        // Verify multiple tables were created
        verifyMultiTables(database);

        repository.shutdown();
        System.out.println("✓ Multi-table mode test completed\n");
    }

    private static void testCustomConfiguration() throws Exception {
        System.out.println("\n3. TESTING CUSTOM CONFIGURATION");
        System.out.println("================================");

        // Setup database
        String database = "custom_config_test";
        setupDatabase(database);

        // Test hourly granularity with multi-table mode
        ShardConfig config = ShardConfig.builder()
            .shardId("shard1")
            .host(HOST)
            .port(PORT)
            .database(database)
            .username(USERNAME)
            .password(PASSWORD)
            .enabled(true)
            .build();

        SplitVerseRepository<SubscriberEntity, LocalDateTime> repository =
            SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                .withSingleShard(config)
                .withEntityClass(SubscriberEntity.class)
                .withRepositoryMode(RepositoryMode.MULTI_TABLE)
                .withTableGranularity(GenericMultiTableRepository.TableGranularity.HOURLY)  // Hourly tables
                .withRetentionDays(1)  // Keep only 1 day of hourly tables
                .withCharset("utf8")
                .withCollation("utf8_general_ci")
                .build();

        // Insert test data for different hours
        for (int hourOffset = -3; hourOffset <= 3; hourOffset++) {
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(UUID.randomUUID().toString());
            subscriber.setMsisdn("123456" + hourOffset);
            // subscriber.setEmail("test" + hourOffset + "@example.com"); // Email field doesn't exist
            subscriber.setCreatedAt(LocalDateTime.now().plusHours(hourOffset));
            repository.insert(subscriber);
        }

        // Verify hourly tables
        verifyHourlyTables(database);

        repository.shutdown();
        System.out.println("✓ Custom configuration test completed\n");
    }

    private static void setupDatabase(String database) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC", HOST, PORT);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP DATABASE IF EXISTS " + database);
            stmt.execute("CREATE DATABASE " + database);
            System.out.println("  Created database: " + database);
        }
    }

    private static void verifyPartitionedTable(String database) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, database);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Check for partitioned table
            String query = """
                SELECT table_name, partition_name
                FROM information_schema.partitions
                WHERE table_schema = '%s'
                AND partition_name IS NOT NULL
                ORDER BY partition_ordinal_position
                LIMIT 5
                """.formatted(database);

            System.out.println("  Partitions found:");
            try (ResultSet rs = stmt.executeQuery(query)) {
                int count = 0;
                while (rs.next() && count < 5) {
                    System.out.println("    - Table: " + rs.getString("table_name") +
                                     ", Partition: " + rs.getString("partition_name"));
                    count++;
                }
                if (count == 0) {
                    throw new RuntimeException("No partitions found!");
                }
            }
        }
    }

    private static void verifyMultiTables(String database) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, database);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Check for multiple daily tables
            String query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name LIKE 'subscribers_%%'
                ORDER BY table_name
                """.formatted(database);

            System.out.println("  Daily tables found:");
            try (ResultSet rs = stmt.executeQuery(query)) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("    - " + rs.getString("table_name"));
                    count++;
                }
                if (count < 3) {
                    throw new RuntimeException("Expected at least 3 daily tables, found: " + count);
                }
                System.out.println("  Total tables: " + count);
            }
        }
    }

    private static void verifyHourlyTables(String database) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, database);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Check for hourly tables
            String query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name LIKE 'subscribers_%%'
                ORDER BY table_name
                LIMIT 10
                """.formatted(database);

            System.out.println("  Hourly tables found:");
            try (ResultSet rs = stmt.executeQuery(query)) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("    - " + rs.getString("table_name"));
                    count++;
                }
                if (count < 3) {
                    throw new RuntimeException("Expected at least 3 hourly tables, found: " + count);
                }
            }
        }
    }
}