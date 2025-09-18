package com.telcobright.splitverse.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for setting up and cleaning test databases.
 */
public class TestDatabaseSetup {

    private static final String MYSQL_HOST = "127.0.0.1";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    private static final String TEST_DB_PREFIX = "split_verse_test_";

    /**
     * Create test databases for sharding tests.
     * Creates 1-3 databases based on shardCount.
     */
    public static List<String> createTestDatabases(int shardCount) throws SQLException {
        List<String> databases = new ArrayList<>();

        try (Connection conn = getConnection(null)) {
            Statement stmt = conn.createStatement();

            for (int i = 0; i < shardCount; i++) {
                String dbName = TEST_DB_PREFIX + "shard_" + i;
                databases.add(dbName);

                // Drop if exists and recreate
                stmt.execute("DROP DATABASE IF EXISTS " + dbName);
                stmt.execute("CREATE DATABASE " + dbName);
                System.out.println("Created test database: " + dbName);
            }
        }

        return databases;
    }

    /**
     * Create a single test database with a specific name.
     */
    public static void createTestDatabase(String dbName) throws SQLException {
        try (Connection conn = getConnection(null)) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP DATABASE IF EXISTS " + dbName);
            stmt.execute("CREATE DATABASE " + dbName);
            System.out.println("Created test database: " + dbName);
        }
    }

    /**
     * Clean up all test databases.
     */
    public static void cleanupTestDatabases() {
        try (Connection conn = getConnection(null)) {
            Statement stmt = conn.createStatement();

            // Get list of all databases
            var rs = stmt.executeQuery("SHOW DATABASES LIKE '" + TEST_DB_PREFIX + "%'");
            List<String> databases = new ArrayList<>();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }

            // Drop each test database
            for (String db : databases) {
                try {
                    stmt.execute("DROP DATABASE IF EXISTS " + db);
                    System.out.println("Dropped test database: " + db);
                } catch (SQLException e) {
                    System.err.println("Failed to drop database " + db + ": " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println("Failed to cleanup test databases: " + e.getMessage());
        }
    }

    /**
     * Get a connection to MySQL.
     */
    private static Connection getConnection(String database) throws SQLException {
        String url = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT;
        if (database != null) {
            url += "/" + database;
        }
        url += "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC";

        return DriverManager.getConnection(url, MYSQL_USER, MYSQL_PASSWORD);
    }

    /**
     * Verify MySQL connectivity.
     */
    public static boolean verifyMySQLConnection() {
        try (Connection conn = getConnection(null)) {
            return conn != null && !conn.isClosed();
        } catch (SQLException e) {
            System.err.println("MySQL connection failed: " + e.getMessage());
            System.err.println("Please ensure MySQL is running at " + MYSQL_HOST + ":" + MYSQL_PORT);
            System.err.println("With credentials: " + MYSQL_USER + "/" + MYSQL_PASSWORD);
            return false;
        }
    }

    /**
     * Get JDBC URL for a test database.
     */
    public static String getJdbcUrl(String database) {
        return "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + database +
               "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC";
    }
}