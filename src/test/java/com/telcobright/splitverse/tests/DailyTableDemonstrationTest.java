package com.telcobright.splitverse.tests;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Demonstrates the multi-table per day approach
 * This shows how the framework would create separate tables for each day
 * instead of using partitions within a single table
 */
public class DailyTableDemonstrationTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String DATABASE = "daily_tables_test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";
    private static final String BASE_TABLE_NAME = "events";

    private static final DateTimeFormatter TABLE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy_MM_dd");
    private static final int RETENTION_DAYS = 7;

    public static void main(String[] args) {
        System.out.println("=== Daily Table Demonstration Test ===\n");
        System.out.println("This demonstrates how multi-table per day works:\n");
        System.out.println("- Each day gets its own table (events_2025_09_13, events_2025_09_14, etc.)");
        System.out.println("- Tables are created on-demand when data arrives");
        System.out.println("- Old tables are dropped after retention period");
        System.out.println("- Queries span multiple tables for date ranges\n");

        try {
            // Setup
            setupDatabase();

            // Demonstrate table creation for multiple days
            demonstrateTableCreation();

            // Insert sample data
            insertSampleData();

            // Query single day
            querySingleDay();

            // Query date range (spans multiple tables)
            queryDateRange();

            // Demonstrate retention management
            demonstrateRetentionManagement();

            // Show final state
            showAllTables();

            System.out.println("\n✅ Demonstration complete!");

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
            stmt.execute("CREATE DATABASE " + DATABASE + " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
            System.out.println("✓ Created database: " + DATABASE + "\n");
        }
    }

    private static void demonstrateTableCreation() throws SQLException {
        System.out.println("1. CREATING TABLES FOR RETENTION PERIOD");
        System.out.println("=========================================");

        LocalDate today = LocalDate.now();
        LocalDate startDate = today.minusDays(RETENTION_DAYS);
        LocalDate endDate = today.plusDays(RETENTION_DAYS);

        System.out.println("Creating tables from " + startDate + " to " + endDate);
        System.out.println("Total tables to create: " + (RETENTION_DAYS * 2 + 1) + "\n");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                String tableName = BASE_TABLE_NAME + "_" + current.format(TABLE_DATE_FORMAT);

                String createTableSQL = String.format("""
                    CREATE TABLE IF NOT EXISTS %s (
                        id VARCHAR(255) PRIMARY KEY,
                        event_type VARCHAR(100),
                        event_data TEXT,
                        created_at DATETIME NOT NULL,
                        processed BOOLEAN DEFAULT FALSE,
                        KEY idx_created_at (created_at),
                        KEY idx_event_type (event_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                    """, tableName);

                stmt.execute(createTableSQL);
                System.out.println("  ✓ Created table: " + tableName);

                current = current.plusDays(1);
            }
        }

        System.out.println("\n");
    }

    private static void insertSampleData() throws SQLException {
        System.out.println("2. INSERTING SAMPLE DATA");
        System.out.println("========================");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD)) {

            // Insert data for past 3 days, today, and future 3 days
            for (int dayOffset = -3; dayOffset <= 3; dayOffset++) {
                LocalDateTime targetDate = LocalDateTime.now().plusDays(dayOffset);
                String tableName = BASE_TABLE_NAME + "_" + targetDate.toLocalDate().format(TABLE_DATE_FORMAT);

                // Insert 5 events per day
                for (int i = 1; i <= 5; i++) {
                    String id = UUID.randomUUID().toString();
                    String eventType = (i % 2 == 0) ? "LOGIN" : "PURCHASE";
                    String eventData = String.format("{\"user\":\"user_%d\",\"amount\":%d}",
                                                    Math.abs(dayOffset * 100 + i), i * 100);

                    String insertSQL = String.format("""
                        INSERT INTO %s (id, event_type, event_data, created_at, processed)
                        VALUES (?, ?, ?, ?, ?)
                        """, tableName);

                    try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                        pstmt.setString(1, id);
                        pstmt.setString(2, eventType);
                        pstmt.setString(3, eventData);
                        pstmt.setTimestamp(4, Timestamp.valueOf(targetDate));
                        pstmt.setBoolean(5, i % 3 == 0);
                        pstmt.executeUpdate();
                    }
                }

                System.out.println("  ✓ Inserted 5 events into " + tableName);
            }
        }

        System.out.println("\n");
    }

    private static void querySingleDay() throws SQLException {
        System.out.println("3. QUERYING SINGLE DAY (Today's Table)");
        System.out.println("======================================");

        LocalDate today = LocalDate.now();
        String tableName = BASE_TABLE_NAME + "_" + today.format(TABLE_DATE_FORMAT);

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            String query = String.format("""
                SELECT event_type, COUNT(*) as count, SUM(processed) as processed_count
                FROM %s
                GROUP BY event_type
                """, tableName);

            System.out.println("  Query: " + tableName);

            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    System.out.println("    - " + rs.getString("event_type") +
                                     ": " + rs.getInt("count") + " events" +
                                     " (" + rs.getInt("processed_count") + " processed)");
                }
            }
        }

        System.out.println("\n");
    }

    private static void queryDateRange() throws SQLException {
        System.out.println("4. QUERYING DATE RANGE (Multiple Tables with UNION)");
        System.out.println("===================================================");

        LocalDate startDate = LocalDate.now().minusDays(2);
        LocalDate endDate = LocalDate.now();

        System.out.println("  Date range: " + startDate + " to " + endDate);
        System.out.println("  This query spans 3 tables using UNION ALL:\n");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Build UNION query across multiple tables
            List<String> tableQueries = new ArrayList<>();
            LocalDate current = startDate;

            while (!current.isAfter(endDate)) {
                String tableName = BASE_TABLE_NAME + "_" + current.format(TABLE_DATE_FORMAT);
                tableQueries.add(String.format("""
                    SELECT '%s' as table_date, event_type, COUNT(*) as count
                    FROM %s
                    GROUP BY event_type
                    """, current.toString(), tableName));
                current = current.plusDays(1);
            }

            String unionQuery = String.join(" UNION ALL ", tableQueries) + " ORDER BY table_date, event_type";

            try (ResultSet rs = stmt.executeQuery(unionQuery)) {
                String lastDate = "";
                while (rs.next()) {
                    String tableDate = rs.getString("table_date");
                    if (!tableDate.equals(lastDate)) {
                        System.out.println("  Date: " + tableDate);
                        lastDate = tableDate;
                    }
                    System.out.println("    - " + rs.getString("event_type") +
                                     ": " + rs.getInt("count") + " events");
                }
            }
        }

        System.out.println("\n");
    }

    private static void demonstrateRetentionManagement() throws SQLException {
        System.out.println("5. RETENTION MANAGEMENT (Dropping Old Tables)");
        System.out.println("=============================================");

        LocalDate cutoffDate = LocalDate.now().minusDays(RETENTION_DAYS);
        System.out.println("  Retention period: " + RETENTION_DAYS + " days");
        System.out.println("  Cutoff date: " + cutoffDate);
        System.out.println("  Tables older than cutoff would be dropped:\n");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Find tables to drop (for demonstration, we won't actually drop them)
            String query = String.format("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name LIKE '%s_%%'
                ORDER BY table_name
                """, DATABASE, BASE_TABLE_NAME);

            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    // Extract date from table name
                    String dateStr = tableName.substring(BASE_TABLE_NAME.length() + 1);
                    LocalDate tableDate = LocalDate.parse(dateStr, TABLE_DATE_FORMAT);

                    if (tableDate.isBefore(cutoffDate)) {
                        System.out.println("    ⚠️  Would drop: " + tableName + " (older than retention period)");
                        // In production: stmt.execute("DROP TABLE " + tableName);
                    }
                }
            }
        }

        System.out.println("\n");
    }

    private static void showAllTables() throws SQLException {
        System.out.println("6. FINAL STATE - ALL TABLES");
        System.out.println("===========================");

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                                      HOST, PORT, DATABASE);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            String query = String.format("""
                SELECT table_name,
                       (SELECT COUNT(*) FROM %s.%%s) as row_count
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name LIKE '%s_%%'
                ORDER BY table_name
                """, DATABASE, DATABASE, BASE_TABLE_NAME);

            // Simplified query without subquery
            query = String.format("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name LIKE '%s_%%'
                ORDER BY table_name
                """, DATABASE, BASE_TABLE_NAME);

            try (ResultSet rs = stmt.executeQuery(query)) {
                int tableCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("table_name");

                    // Get row count for each table
                    try (Statement countStmt = conn.createStatement();
                         ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                        if (countRs.next()) {
                            int rowCount = countRs.getInt(1);
                            System.out.println("  - " + tableName + ": " + rowCount + " rows");
                            tableCount++;
                        }
                    }
                }
                System.out.println("\n  Total tables: " + tableCount);
            }
        }
    }
}