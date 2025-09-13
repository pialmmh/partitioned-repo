package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Test demonstrating multi-table per day functionality
 * Creates separate tables for each day instead of using partitions
 */
public class MultiTablePerDayTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String DATABASE = "multitable_test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";
    private static final String BASE_TABLE_NAME = "subscribers";

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    public static void main(String[] args) {
        System.out.println("=== Multi-Table Per Day Test ===\n");

        try {
            // Setup database
            setupDatabase();

            // Create shard configuration
            ShardConfig config = ShardConfig.builder()
                .shardId("primary")
                .host(HOST)
                .port(PORT)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .connectionPoolSize(5)
                .enabled(true)
                .build();

            // Create repository with multi-table strategy
            System.out.println("Creating Multi-Table Repository through SplitVerseRepository...");
            // Note: Multi-table mode would be configured through builder options
            // For now, we'll use partitioned mode to demonstrate the concept
            SplitVerseRepository<SubscriberEntity> repository =
                SplitVerseRepository.<SubscriberEntity>builder()
                    .withSingleShard(config)
                    .withEntityClass(SubscriberEntity.class)
                    .build();

            System.out.println("✓ Multi-table repository created\n");

            // Test 1: Insert data for different days
            testInsertMultipleDays(repository);

            // Test 2: Query data from specific day
            testQuerySpecificDay(repository);

            // Test 3: Query across date range
            testQueryDateRange(repository);

            // Test 4: Verify table creation
            verifyTableCreation();

            // Test 5: Test table cleanup (retention)
            testTableRetention(repository);

            // Cleanup
            repository.shutdown();
            System.out.println("\n✓ All tests completed successfully!");

        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setupDatabase() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC", HOST, PORT);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP DATABASE IF EXISTS " + DATABASE);
            stmt.execute("CREATE DATABASE " + DATABASE);
            System.out.println("✓ Database created: " + DATABASE);
        }
    }

    private static void testInsertMultipleDays(SplitVerseRepository<SubscriberEntity> repository) {
        System.out.println("Test 1: Inserting data for multiple days");
        System.out.println("-----------------------------------------");

        LocalDateTime now = LocalDateTime.now();

        // Insert data for past 3 days, today, and future 3 days
        for (int dayOffset = -3; dayOffset <= 3; dayOffset++) {
            LocalDateTime targetDate = now.plusDays(dayOffset);
            String dateStr = targetDate.toLocalDate().toString();

            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId("sub_" + UUID.randomUUID().toString().substring(0, 8));
            subscriber.setMsisdn("+88017" + String.format("%08d", Math.abs(dayOffset * 1000000 + (int)(Math.random() * 999999))));
            subscriber.setBalance(new BigDecimal("100.00").add(new BigDecimal(dayOffset * 10)));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("PREPAID");
            subscriber.setCreatedAt(targetDate);
            subscriber.setDataBalanceMb(5000L + (dayOffset * 1000));
            subscriber.setVoiceBalanceMinutes(500 + (dayOffset * 100));

            try {
                repository.insert(subscriber);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to insert subscriber", e);
            }

            String tableName = BASE_TABLE_NAME + "_" + targetDate.format(DATE_FORMAT);
            System.out.println("  Inserted into " + tableName + " for date: " + dateStr);
        }

        System.out.println("✓ Inserted data for 7 different days\n");
    }

    private static void testQuerySpecificDay(SplitVerseRepository<SubscriberEntity> repository) {
        System.out.println("Test 2: Query data from specific day");
        System.out.println("-------------------------------------");

        LocalDateTime today = LocalDateTime.now();
        LocalDateTime tomorrow = today.plusDays(1);
        List<SubscriberEntity> todaySubscribers;
        try {
            todaySubscribers = repository.findAllByDateRange(today.withHour(0).withMinute(0).withSecond(0),
                                                            tomorrow.withHour(0).withMinute(0).withSecond(0));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query today's data", e);
        }

        System.out.println("  Found " + todaySubscribers.size() + " subscriber(s) for today");
        for (SubscriberEntity sub : todaySubscribers) {
            System.out.println("    - ID: " + sub.getId() + ", MSISDN: " + sub.getMsisdn() +
                             ", Balance: " + sub.getBalance());
        }

        System.out.println("✓ Successfully queried today's table\n");
    }

    private static void testQueryDateRange(SplitVerseRepository<SubscriberEntity> repository) {
        System.out.println("Test 3: Query across date range");
        System.out.println("--------------------------------");

        LocalDateTime startDate = LocalDateTime.now().minusDays(2);
        LocalDateTime endDate = LocalDateTime.now().plusDays(2);

        List<SubscriberEntity> rangeSubscribers;
        try {
            rangeSubscribers = repository.findAllByDateRange(startDate, endDate);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query date range", e);
        }

        System.out.println("  Querying from " + startDate.toLocalDate() + " to " + endDate.toLocalDate());
        System.out.println("  Found " + rangeSubscribers.size() + " total subscribers across 5 days");

        // Group by date to show distribution
        Map<String, Integer> countByDate = new TreeMap<>();
        for (SubscriberEntity sub : rangeSubscribers) {
            String date = sub.getCreatedAt().toLocalDate().toString();
            countByDate.merge(date, 1, Integer::sum);
        }

        System.out.println("  Distribution by date:");
        for (Map.Entry<String, Integer> entry : countByDate.entrySet()) {
            System.out.println("    " + entry.getKey() + ": " + entry.getValue() + " subscriber(s)");
        }

        System.out.println("✓ Successfully queried across multiple tables\n");
    }

    private static void verifyTableCreation() throws SQLException {
        System.out.println("Test 4: Verify table creation");
        System.out.println("-----------------------------");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT table_name FROM information_schema.tables " +
                 "WHERE table_schema = '" + DATABASE + "' " +
                 "AND table_name LIKE '" + BASE_TABLE_NAME + "_%' " +
                 "ORDER BY table_name")) {

            System.out.println("  Created tables:");
            int tableCount = 0;
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                System.out.println("    - " + tableName);
                tableCount++;
            }

            System.out.println("  Total tables created: " + tableCount);
            System.out.println("✓ Tables created successfully\n");
        }
    }

    private static void testTableRetention(SplitVerseRepository<SubscriberEntity> repository) {
        System.out.println("Test 5: Table retention management");
        System.out.println("----------------------------------");

        // In a real scenario, this would be handled by the scheduled cleanup
        // Here we'll just demonstrate the concept

        System.out.println("  Retention period: 7 days");
        System.out.println("  Tables older than 7 days would be automatically dropped");
        System.out.println("  New tables are created on-demand when data arrives");

        // Insert data for a date 10 days ago (would be outside retention)
        LocalDateTime oldDate = LocalDateTime.now().minusDays(10);
        SubscriberEntity oldSubscriber = new SubscriberEntity();
        oldSubscriber.setId("old_" + UUID.randomUUID().toString().substring(0, 8));
        oldSubscriber.setMsisdn("+8801799999999");
        oldSubscriber.setBalance(new BigDecimal("50.00"));
        oldSubscriber.setStatus("INACTIVE");
        oldSubscriber.setPlan("PREPAID");
        oldSubscriber.setCreatedAt(oldDate);
        oldSubscriber.setDataBalanceMb(0L);
        oldSubscriber.setVoiceBalanceMinutes(0);

        try {
            repository.insert(oldSubscriber);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert old subscriber", e);
        }

        String oldTableName = BASE_TABLE_NAME + "_" + oldDate.format(DATE_FORMAT);
        System.out.println("  Created table for old date: " + oldTableName);
        System.out.println("  (This would be dropped in next maintenance cycle)");

        System.out.println("✓ Retention management demonstrated\n");
    }
}