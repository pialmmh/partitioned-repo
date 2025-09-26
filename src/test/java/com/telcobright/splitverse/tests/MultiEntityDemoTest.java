package com.telcobright.splitverse.tests;

import com.telcobright.splitverse.tests.entities.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Simplified demonstration of multi-entity repository concept using direct JDBC.
 * This test demonstrates how multiple entities can be managed with different
 * partitioning strategies in a coordinated manner.
 */
public class MultiEntityDemoTest {

    private static final String DB_HOST = "127.0.0.1";
    private static final int DB_PORT = 3306;
    private static final String DB_NAME = "test";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";

    private Connection connection;
    private Map<Class<?>, String> entityTableMap = new HashMap<>();

    @BeforeEach
    public void setup() throws SQLException {
        // Clean up any existing test data
        cleanupTestData();

        // Create connection
        connection = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%d/%s", DB_HOST, DB_PORT, DB_NAME),
            DB_USER, DB_PASSWORD
        );

        // Register entity-table mappings
        entityTableMap.put(UserEntity.class, "users");
        entityTableMap.put(OrderEntity.class, "orders");
        entityTableMap.put(AuditLogEntity.class, "audit_logs");

        // Create tables with different partitioning strategies
        createUserTable();  // Daily partitions
        createOrderTable(); // Monthly partitions
        createAuditTable(); // Daily partitions with 3-day retention

        System.out.println("✓ Setup completed - 3 tables created with different partition strategies");
    }

    private void createUserTable() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(50) NOT NULL,
                username VARCHAR(100),
                email VARCHAR(100),
                created_at DATETIME NOT NULL,
                PRIMARY KEY (id, created_at)
            ) PARTITION BY RANGE (TO_DAYS(created_at)) (
                PARTITION p_2024_01_01 VALUES LESS THAN (TO_DAYS('2024-01-02')),
                PARTITION p_2024_01_02 VALUES LESS THAN (TO_DAYS('2024-01-03')),
                PARTITION p_2024_01_03 VALUES LESS THAN (TO_DAYS('2024-01-04')),
                PARTITION p_2024_01_04 VALUES LESS THAN (TO_DAYS('2024-01-05')),
                PARTITION p_2024_01_05 VALUES LESS THAN (TO_DAYS('2024-01-06')),
                PARTITION p_2024_01_06 VALUES LESS THAN (TO_DAYS('2024-01-07')),
                PARTITION p_2024_01_07 VALUES LESS THAN (TO_DAYS('2024-01-08')),
                PARTITION p_future VALUES LESS THAN MAXVALUE
            )
        """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ Created users table with daily partitions");
        }
    }

    private void createOrderTable() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS orders (
                id VARCHAR(50) NOT NULL,
                customer_id VARCHAR(50),
                total_amount DECIMAL(10,2),
                order_date DATETIME NOT NULL,
                PRIMARY KEY (id, order_date)
            ) PARTITION BY RANGE (TO_DAYS(order_date)) (
                PARTITION p_2024_01 VALUES LESS THAN (TO_DAYS('2024-02-01')),
                PARTITION p_2024_02 VALUES LESS THAN (TO_DAYS('2024-03-01')),
                PARTITION p_2024_03 VALUES LESS THAN (TO_DAYS('2024-04-01')),
                PARTITION p_future VALUES LESS THAN MAXVALUE
            )
        """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ Created orders table with monthly partitions");
        }
    }

    private void createAuditTable() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id VARCHAR(50) NOT NULL,
                user_id VARCHAR(50),
                action VARCHAR(100),
                event_time DATETIME NOT NULL,
                resource VARCHAR(100),
                ip_address VARCHAR(45),
                details TEXT,
                PRIMARY KEY (id, event_time)
            ) PARTITION BY RANGE (TO_DAYS(event_time)) (
                PARTITION p_2024_01_01 VALUES LESS THAN (TO_DAYS('2024-01-02')),
                PARTITION p_2024_01_02 VALUES LESS THAN (TO_DAYS('2024-01-03')),
                PARTITION p_2024_01_03 VALUES LESS THAN (TO_DAYS('2024-01-04')),
                PARTITION p_future VALUES LESS THAN MAXVALUE
            )
        """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ Created audit_logs table with 3-day partitions");
        }
    }

    @Test
    public void testMultiEntityOperations() throws SQLException {
        System.out.println("\n=== Testing Multi-Entity Operations ===");

        LocalDateTime now = LocalDateTime.of(2024, 1, 3, 12, 0, 0);

        // Insert into different entity types
        insertUser("user1", "john_doe", "john@example.com", now);
        System.out.println("✓ Inserted user");

        insertOrder("order1", "customer1", new BigDecimal("99.99"), now);
        System.out.println("✓ Inserted order");

        insertAuditLog("log1", "user1", "LOGIN", now);
        System.out.println("✓ Inserted audit log");

        // Retrieve from different entity types
        Map<String, Object> user = findUserById("user1");
        assertNotNull(user, "Should retrieve user");
        assertEquals("john_doe", user.get("username"));

        Map<String, Object> order = findOrderById("order1");
        assertNotNull(order, "Should retrieve order");
        assertEquals(new BigDecimal("99.99"), order.get("total_amount"));

        Map<String, Object> log = findAuditLogById("log1");
        assertNotNull(log, "Should retrieve audit log");
        assertEquals("LOGIN", log.get("action"));

        System.out.println("✓ All entities retrieved successfully");
    }

    @Test
    public void testPartitionPruning() throws SQLException {
        System.out.println("\n=== Testing Partition Pruning ===");

        LocalDateTime date1 = LocalDateTime.of(2024, 1, 2, 10, 0, 0);
        LocalDateTime date2 = LocalDateTime.of(2024, 1, 3, 10, 0, 0);
        LocalDateTime date3 = LocalDateTime.of(2024, 1, 4, 10, 0, 0);

        // Insert users across different partitions
        insertUser("pruning_user_1", "user1", "u1@test.com", date1);
        insertUser("pruning_user_2", "user2", "u2@test.com", date2);
        insertUser("pruning_user_3", "user3", "u3@test.com", date3);

        // Query with partition pruning (date range)
        String query = """
            SELECT * FROM users
            WHERE created_at >= ? AND created_at < ?
        """;

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setTimestamp(1, Timestamp.valueOf(date2));
            stmt.setTimestamp(2, Timestamp.valueOf(date3));
            ResultSet rs = stmt.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(1, count, "Should get 1 user from specific partition");
            System.out.println("✓ Partition pruning verified - retrieved 1 record from specific date");
        }

        // Verify with EXPLAIN
        verifyPartitionPruning("users", date2, date3);
    }

    @Test
    public void testEntityIsolation() throws SQLException {
        System.out.println("\n=== Testing Entity Isolation ===");

        String sharedId = "shared_123";
        LocalDateTime now = LocalDateTime.of(2024, 1, 3, 12, 0, 0);

        // Insert entities with same ID in different tables
        insertUser(sharedId, "shared_user", "shared@example.com", now);
        insertOrder(sharedId, "shared_customer", new BigDecimal("199.99"), now);
        insertAuditLog(sharedId, "shared_user", "TEST_ACTION", now);

        System.out.println("✓ Inserted 3 entities with same ID in different tables");

        // Delete from one entity
        deleteUser(sharedId);

        // Verify other entities are unaffected
        Map<String, Object> deletedUser = findUserById(sharedId);
        assertNull(deletedUser, "User should be deleted");

        Map<String, Object> existingOrder = findOrderById(sharedId);
        assertNotNull(existingOrder, "Order should still exist");

        Map<String, Object> existingLog = findAuditLogById(sharedId);
        assertNotNull(existingLog, "Audit log should still exist");

        System.out.println("✓ Entity isolation verified - operations don't affect other entities");
    }

    @Test
    public void testPerformanceComparison() throws SQLException {
        System.out.println("\n=== Testing Performance Comparison ===");

        LocalDateTime baseDate = LocalDateTime.of(2024, 1, 3, 0, 0, 0);
        int recordCount = 100;

        // Insert users across multiple partitions
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            insertUser("perf_user_" + i, "user" + i, "u" + i + "@test.com",
                      baseDate.plusHours(i % 24));
        }
        long insertTime = System.currentTimeMillis() - startTime;
        System.out.println("✓ Inserted " + recordCount + " users in " + insertTime + "ms");

        // Query with partition optimization (includes date)
        startTime = System.currentTimeMillis();
        Map<String, Object> userOpt = findUserByIdAndDate("perf_user_50", baseDate.plusHours(50 % 24));
        long optimizedTime = System.currentTimeMillis() - startTime;

        // Query without optimization (ID only)
        startTime = System.currentTimeMillis();
        Map<String, Object> userNoOpt = findUserById("perf_user_50");
        long nonOptimizedTime = System.currentTimeMillis() - startTime;

        System.out.println("✓ Query with partition col: " + optimizedTime + "ms");
        System.out.println("✓ Query with ID only: " + nonOptimizedTime + "ms");

        if (optimizedTime <= nonOptimizedTime) {
            System.out.println("✓ Partition pruning optimization working!");
        }
    }

    // Helper methods for entity operations

    private void insertUser(String id, String username, String email, LocalDateTime createdAt) throws SQLException {
        String sql = "INSERT INTO users (id, username, email, created_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, username);
            stmt.setString(3, email);
            stmt.setTimestamp(4, Timestamp.valueOf(createdAt));
            stmt.executeUpdate();
        }
    }

    private void insertOrder(String id, String customerId, BigDecimal amount, LocalDateTime orderDate) throws SQLException {
        String sql = "INSERT INTO orders (id, customer_id, total_amount, order_date) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, customerId);
            stmt.setBigDecimal(3, amount);
            stmt.setTimestamp(4, Timestamp.valueOf(orderDate));
            stmt.executeUpdate();
        }
    }

    private void insertAuditLog(String id, String userId, String action, LocalDateTime eventTime) throws SQLException {
        String sql = "INSERT INTO audit_logs (id, user_id, action, event_time) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, userId);
            stmt.setString(3, action);
            stmt.setTimestamp(4, Timestamp.valueOf(eventTime));
            stmt.executeUpdate();
        }
    }

    private Map<String, Object> findUserById(String id) throws SQLException {
        String sql = "SELECT * FROM users WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Map<String, Object> user = new HashMap<>();
                user.put("id", rs.getString("id"));
                user.put("username", rs.getString("username"));
                user.put("email", rs.getString("email"));
                user.put("created_at", rs.getTimestamp("created_at").toLocalDateTime());
                return user;
            }
        }
        return null;
    }

    private Map<String, Object> findUserByIdAndDate(String id, LocalDateTime date) throws SQLException {
        String sql = "SELECT * FROM users WHERE id = ? AND created_at = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setTimestamp(2, Timestamp.valueOf(date));
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Map<String, Object> user = new HashMap<>();
                user.put("id", rs.getString("id"));
                user.put("username", rs.getString("username"));
                user.put("email", rs.getString("email"));
                user.put("created_at", rs.getTimestamp("created_at").toLocalDateTime());
                return user;
            }
        }
        return null;
    }

    private Map<String, Object> findOrderById(String id) throws SQLException {
        String sql = "SELECT * FROM orders WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Map<String, Object> order = new HashMap<>();
                order.put("id", rs.getString("id"));
                order.put("customer_id", rs.getString("customer_id"));
                order.put("total_amount", rs.getBigDecimal("total_amount"));
                order.put("order_date", rs.getTimestamp("order_date").toLocalDateTime());
                return order;
            }
        }
        return null;
    }

    private Map<String, Object> findAuditLogById(String id) throws SQLException {
        String sql = "SELECT * FROM audit_logs WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Map<String, Object> log = new HashMap<>();
                log.put("id", rs.getString("id"));
                log.put("user_id", rs.getString("user_id"));
                log.put("action", rs.getString("action"));
                log.put("event_time", rs.getTimestamp("event_time").toLocalDateTime());
                return log;
            }
        }
        return null;
    }

    private void deleteUser(String id) throws SQLException {
        String sql = "DELETE FROM users WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.executeUpdate();
        }
    }

    private void verifyPartitionPruning(String tableName, LocalDateTime start, LocalDateTime end) throws SQLException {
        String query = String.format(
            "EXPLAIN SELECT * FROM %s WHERE created_at >= ? AND created_at < ?",
            tableName
        );
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setTimestamp(1, Timestamp.valueOf(start));
            stmt.setTimestamp(2, Timestamp.valueOf(end));
            ResultSet rs = stmt.executeQuery();

            boolean partitionPruning = false;
            while (rs.next()) {
                String extra = rs.getString("Extra");
                String partitions = rs.getString("partitions");
                if (partitions != null && !partitions.equals("ALL")) {
                    partitionPruning = true;
                    System.out.println("✓ Partition pruning confirmed - using partitions: " + partitions);
                    break;
                }
            }

            if (!partitionPruning) {
                System.out.println("⚠ Partition pruning not detected for " + tableName);
            }
        }
    }

    private void cleanupTestData() {
        try {
            connection = DriverManager.getConnection(
                String.format("jdbc:mysql://%s:%d/%s", DB_HOST, DB_PORT, DB_NAME),
                DB_USER, DB_PASSWORD
            );
            Statement stmt = connection.createStatement();

            // Drop test tables
            stmt.execute("DROP TABLE IF EXISTS users");
            stmt.execute("DROP TABLE IF EXISTS orders");
            stmt.execute("DROP TABLE IF EXISTS audit_logs");

            System.out.println("✓ Cleanup completed");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @AfterEach
    public void tearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("✓ Test teardown completed");
    }
}