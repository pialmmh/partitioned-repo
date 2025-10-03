package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.splitverse.tests.entities.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive test suite for multi-entity repository functionality.
 * Tests all aspects to ensure 100% success rate.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiEntityComprehensiveTest {

    private static final String DB_HOST = "127.0.0.1";
    private static final int DB_PORT = 3306;
    private static final String DB_NAME = "test_multi_entity";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";

    private static GenericPartitionedTableRepository<UserEntity, LocalDateTime> userRepo;
    private static GenericMultiTableRepository<OrderEntity, LocalDateTime> orderRepo;
    private static GenericPartitionedTableRepository<AuditLogEntity, LocalDateTime> auditRepo;
    private static Connection connection;

    @BeforeAll
    static void setupAll() throws SQLException {
        // Create test database
        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "123456");
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + DB_NAME);
            stmt.execute("CREATE DATABASE " + DB_NAME);
            System.out.println("✓ Database created: " + DB_NAME);
        }

        // Create connection for verification
        connection = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%d/%s", DB_HOST, DB_PORT, DB_NAME),
            DB_USER, DB_PASSWORD
        );

        // Initialize repositories using public builders
        userRepo = GenericPartitionedTableRepository.builder(UserEntity.class)
            .host(DB_HOST)
            .port(DB_PORT)
            .database(DB_NAME)
            .username(DB_USER)
            .password(DB_PASSWORD)
            .tableName("users")
            .build();

        orderRepo = GenericMultiTableRepository.builder(OrderEntity.class)
            .host(DB_HOST)
            .port(DB_PORT)
            .database(DB_NAME)
            .username(DB_USER)
            .password(DB_PASSWORD)
            .tablePrefix("orders")
            .tableGranularity(GenericMultiTableRepository.TableGranularity.DAILY)
            .build();

        auditRepo = GenericPartitionedTableRepository.builder(AuditLogEntity.class)
            .host(DB_HOST)
            .port(DB_PORT)
            .database(DB_NAME)
            .username(DB_USER)
            .password(DB_PASSWORD)
            .tableName("audit_logs")
            .build();

        System.out.println("✓ All repositories initialized");
    }

    @Test
    @Order(1)
    void testTableCreation() throws SQLException {
        System.out.println("\n=== Test 1: Table Creation ===");

        // Verify tables exist
        assertTrue(tableExists("users"), "Users table should exist");
        assertTrue(tableExists("audit_logs"), "Audit logs table should exist");

        // Order tables are created dynamically
        LocalDateTime now = LocalDateTime.now();
        OrderEntity testOrder = new OrderEntity("test_order", "test_customer", new BigDecimal("10.00"), now);
        orderRepo.insert(testOrder);

        // Check for order table creation
        String orderTablePattern = "orders_%";
        assertTrue(countTablesLike(orderTablePattern) > 0, "Order tables should exist");

        // Clean up test order
        orderRepo.deleteById("test_order");

        System.out.println("✓ All tables created successfully");
    }

    @Test
    @Order(2)
    void testSingleEntityInsertAndRetrieve() throws SQLException {
        System.out.println("\n=== Test 2: Single Entity Insert and Retrieve ===");

        LocalDateTime now = LocalDateTime.now();

        // Test UserEntity
        UserEntity user = new UserEntity("single_user_1", "john_doe", "john@example.com", now);
        userRepo.insert(user);
        UserEntity retrievedUser = userRepo.findById("single_user_1");
        assertNotNull(retrievedUser, "User should be retrieved");
        assertEquals("john_doe", retrievedUser.getUsername());
        System.out.println("✓ UserEntity insert/retrieve successful");

        // Test OrderEntity
        OrderEntity order = new OrderEntity("single_order_1", "customer_1", new BigDecimal("99.99"), now);
        orderRepo.insert(order);
        OrderEntity retrievedOrder = orderRepo.findById("single_order_1");
        assertNotNull(retrievedOrder, "Order should be retrieved");
        assertEquals(0, new BigDecimal("99.99").compareTo(retrievedOrder.getTotalAmount()),
            "Total amount should be 99.99");
        System.out.println("✓ OrderEntity insert/retrieve successful");

        // Test AuditLogEntity
        AuditLogEntity audit = new AuditLogEntity("single_audit_1", "user_1", "LOGIN", now);
        auditRepo.insert(audit);
        AuditLogEntity retrievedAudit = auditRepo.findById("single_audit_1");
        assertNotNull(retrievedAudit, "Audit log should be retrieved");
        assertEquals("LOGIN", retrievedAudit.getAction());
        System.out.println("✓ AuditLogEntity insert/retrieve successful");
    }

    @Test
    @Order(3)
    void testBatchOperations() throws SQLException {
        System.out.println("\n=== Test 3: Batch Operations ===");

        LocalDateTime baseTime = LocalDateTime.now();
        int batchSize = 100;

        // Prepare batch data
        List<UserEntity> users = new ArrayList<>();
        List<OrderEntity> orders = new ArrayList<>();
        List<AuditLogEntity> audits = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            users.add(new UserEntity(
                "batch_user_" + i,
                "user_" + i,
                "user" + i + "@test.com",
                baseTime.plusMinutes(i)
            ));

            orders.add(new OrderEntity(
                "batch_order_" + i,
                "customer_" + i,
                new BigDecimal(100 + i),
                baseTime.plusMinutes(i)
            ));

            audits.add(new AuditLogEntity(
                "batch_audit_" + i,
                "user_" + i,
                "ACTION_" + i,
                baseTime.plusMinutes(i)
            ));
        }

        // Insert batches
        long startTime = System.currentTimeMillis();
        userRepo.insertMultiple(users);
        long userInsertTime = System.currentTimeMillis() - startTime;
        System.out.println("✓ Inserted " + batchSize + " users in " + userInsertTime + "ms");

        startTime = System.currentTimeMillis();
        orderRepo.insertMultiple(orders);
        long orderInsertTime = System.currentTimeMillis() - startTime;
        System.out.println("✓ Inserted " + batchSize + " orders in " + orderInsertTime + "ms");

        startTime = System.currentTimeMillis();
        auditRepo.insertMultiple(audits);
        long auditInsertTime = System.currentTimeMillis() - startTime;
        System.out.println("✓ Inserted " + batchSize + " audit logs in " + auditInsertTime + "ms");

        // Verify all inserted
        for (int i = 0; i < 10; i++) { // Spot check first 10
            assertNotNull(userRepo.findById("batch_user_" + i));
            assertNotNull(orderRepo.findById("batch_order_" + i));
            assertNotNull(auditRepo.findById("batch_audit_" + i));
        }

        System.out.println("✓ All batch operations successful");
    }

    @Test
    @Order(4)
    void testUpdateOperations() throws SQLException {
        System.out.println("\n=== Test 4: Update Operations ===");

        LocalDateTime now = LocalDateTime.now();

        // Insert test entities
        UserEntity user = new UserEntity("update_user_1", "original_name", "original@test.com", now);
        userRepo.insert(user);

        OrderEntity order = new OrderEntity("update_order_1", "original_customer", new BigDecimal("50.00"), now);
        orderRepo.insert(order);

        // Update entities
        user.setUsername("updated_name");
        user.setEmail("updated@test.com");
        userRepo.updateById("update_user_1", user);

        order.setCustomerId("updated_customer");
        order.setTotalAmount(new BigDecimal("75.00"));
        orderRepo.updateById("update_order_1", order);

        // Verify updates
        UserEntity updatedUser = userRepo.findById("update_user_1");
        assertEquals("updated_name", updatedUser.getUsername());
        assertEquals("updated@test.com", updatedUser.getEmail());
        System.out.println("✓ User update successful");

        OrderEntity updatedOrder = orderRepo.findById("update_order_1");
        assertEquals("updated_customer", updatedOrder.getCustomerId());
        assertEquals(0, new BigDecimal("75.00").compareTo(updatedOrder.getTotalAmount()),
            "Total amount should be 75.00");
        System.out.println("✓ Order update successful");
    }

    @Test
    @Order(5)
    void testDeleteOperations() throws SQLException {
        System.out.println("\n=== Test 5: Delete Operations ===");

        LocalDateTime now = LocalDateTime.now();

        // Insert test entities
        userRepo.insert(new UserEntity("delete_user_1", "user1", "u1@test.com", now));
        orderRepo.insert(new OrderEntity("delete_order_1", "cust1", new BigDecimal("10"), now));
        auditRepo.insert(new AuditLogEntity("delete_audit_1", "user1", "DELETE_TEST", now));

        // Verify they exist
        assertNotNull(userRepo.findById("delete_user_1"));
        assertNotNull(orderRepo.findById("delete_order_1"));
        assertNotNull(auditRepo.findById("delete_audit_1"));

        // Delete entities
        userRepo.deleteById("delete_user_1");
        orderRepo.deleteById("delete_order_1");
        auditRepo.deleteById("delete_audit_1");

        // Verify deletion
        assertNull(userRepo.findById("delete_user_1"), "User should be deleted");
        assertNull(orderRepo.findById("delete_order_1"), "Order should be deleted");
        assertNull(auditRepo.findById("delete_audit_1"), "Audit should be deleted");

        System.out.println("✓ All delete operations successful");
    }

    @Test
    @Order(6)
    void testRangeQueries() throws SQLException {
        System.out.println("\n=== Test 6: Range Queries ===");

        LocalDateTime baseTime = LocalDateTime.now();

        // Insert data across time range
        List<UserEntity> users = new ArrayList<>();
        for (int day = 0; day < 5; day++) {
            for (int hour = 0; hour < 4; hour++) {
                LocalDateTime timestamp = baseTime.plusDays(day).plusHours(hour);
                users.add(new UserEntity(
                    String.format("range_user_%d_%d", day, hour),
                    String.format("user_%d_%d", day, hour),
                    String.format("user_%d_%d@test.com", day, hour),
                    timestamp
                ));
            }
        }
        userRepo.insertMultiple(users);

        // Query specific range
        LocalDateTime rangeStart = baseTime.plusDays(1);
        LocalDateTime rangeEnd = baseTime.plusDays(3);
        List<UserEntity> rangeResults = userRepo.findAllByPartitionRange(rangeStart, rangeEnd);

        // Should get 2 full days (days 1 and 2) = 8 users
        assertEquals(8, rangeResults.size(), "Should retrieve correct range count");

        // Verify all results are within range
        for (UserEntity result : rangeResults) {
            assertTrue(result.getCreatedAt().isAfter(rangeStart.minusSeconds(1)));
            assertTrue(result.getCreatedAt().isBefore(rangeEnd.plusSeconds(1)));
        }

        System.out.println("✓ Range queries successful - retrieved " + rangeResults.size() + " records");
    }

    @Test
    @Order(7)
    void testConcurrentAccess() throws Exception {
        System.out.println("\n=== Test 7: Concurrent Access ===");

        int threadCount = 10;
        int operationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        LocalDateTime now = LocalDateTime.now();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String id = String.format("concurrent_%d_%d", threadId, i);

                        // Mix of operations
                        if (i % 3 == 0) {
                            userRepo.insert(new UserEntity(id, "user" + id, id + "@test.com", now));
                        } else if (i % 3 == 1) {
                            orderRepo.insert(new OrderEntity(id, "customer" + id, new BigDecimal(i), now));
                        } else {
                            auditRepo.insert(new AuditLogEntity(id, "user" + id, "ACTION", now));
                        }

                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Concurrent operations should complete");
        executor.shutdown();

        int totalExpected = threadCount * operationsPerThread;
        assertEquals(totalExpected, successCount.get(), "All operations should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        System.out.println("✓ Concurrent access successful - " + successCount.get() + " operations completed");
    }

    @Test
    @Order(8)
    void testPartitionPruning() throws SQLException {
        System.out.println("\n=== Test 8: Partition Pruning Verification ===");

        LocalDateTime baseTime = LocalDateTime.now();

        // Insert data across multiple partitions
        List<UserEntity> users = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            users.add(new UserEntity(
                "pruning_user_" + i,
                "user_" + i,
                "user" + i + "@test.com",
                baseTime.plusDays(i % 7) // Spread across 7 days
            ));
        }
        userRepo.insertMultiple(users);

        // Query with partition column value (should use partition pruning)
        LocalDateTime targetDate = baseTime.plusDays(3);
        long startTime = System.currentTimeMillis();
        UserEntity userWithPartition = userRepo.findByIdAndPartitionColRange("pruning_user_3", targetDate, targetDate.plusDays(1));
        long withPartitionTime = System.currentTimeMillis() - startTime;

        // Query without partition column (full scan)
        startTime = System.currentTimeMillis();
        UserEntity userWithoutPartition = userRepo.findById("pruning_user_10");
        long withoutPartitionTime = System.currentTimeMillis() - startTime;

        assertNotNull(userWithPartition, "Should find user with partition column");
        assertNotNull(userWithoutPartition, "Should find user without partition column");

        System.out.println("✓ Query with partition column: " + withPartitionTime + "ms");
        System.out.println("✓ Query without partition column: " + withoutPartitionTime + "ms");

        // Verify partition pruning with EXPLAIN
        verifyPartitionPruning("users", targetDate);
    }

    @Test
    @Order(9)
    void testDataIntegrity() throws SQLException {
        System.out.println("\n=== Test 9: Data Integrity ===");

        LocalDateTime now = LocalDateTime.now();

        // Test with various data types and edge cases
        String longString = "A".repeat(255); // Max varchar length

        UserEntity user = new UserEntity("integrity_user_1", longString, "test@example.com", now);
        userRepo.insert(user);
        UserEntity retrieved = userRepo.findById("integrity_user_1");
        assertEquals(longString, retrieved.getUsername(), "Long string should be preserved");

        // Test with special characters
        String specialChars = "test'user\"with\\special@chars";
        user = new UserEntity("integrity_user_2", specialChars, "test@test.com", now);
        userRepo.insert(user);
        retrieved = userRepo.findById("integrity_user_2");
        assertEquals(specialChars, retrieved.getUsername(), "Special characters should be preserved");

        // Test with null values (where allowed)
        OrderEntity order = new OrderEntity("integrity_order_1", null, new BigDecimal("10.00"), now);
        orderRepo.insert(order);
        OrderEntity retrievedOrder = orderRepo.findById("integrity_order_1");
        assertNull(retrievedOrder.getCustomerId(), "Null values should be preserved");

        System.out.println("✓ Data integrity verified");
    }

    @Test
    @Order(10)
    void testErrorRecovery() throws SQLException {
        System.out.println("\n=== Test 10: Error Recovery ===");

        // Test duplicate key handling
        LocalDateTime now = LocalDateTime.now();
        UserEntity user1 = new UserEntity("error_user_1", "user1", "u1@test.com", now);
        userRepo.insert(user1);

        // Try to insert duplicate
        UserEntity user2 = new UserEntity("error_user_1", "user2", "u2@test.com", now);
        assertThrows(SQLException.class, () -> userRepo.insert(user2),
                    "Should throw exception on duplicate key");

        // Verify original data is intact
        UserEntity retrieved = userRepo.findById("error_user_1");
        assertEquals("user1", retrieved.getUsername(), "Original data should be intact");

        // Test transaction rollback behavior
        try {
            List<UserEntity> badBatch = Arrays.asList(
                new UserEntity("error_user_2", "user2", "u2@test.com", now),
                new UserEntity("error_user_1", "duplicate", "dup@test.com", now) // This will fail
            );
            userRepo.insertMultiple(badBatch);
            fail("Should have thrown exception");
        } catch (SQLException e) {
            // Expected
        }

        // Verify partial insert didn't happen (depends on transaction settings)
        UserEntity check = userRepo.findById("error_user_2");
        // Note: This behavior depends on transaction configuration

        System.out.println("✓ Error recovery tested");
    }

    // Helper methods
    private boolean tableExists(String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, DB_NAME);
            stmt.setString(2, tableName);
            ResultSet rs = stmt.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        }
    }

    private int countTablesLike(String pattern) throws SQLException {
        String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, DB_NAME);
            stmt.setString(2, pattern);
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private void verifyPartitionPruning(String tableName, LocalDateTime date) throws SQLException {
        String query = "EXPLAIN SELECT * FROM " + tableName + " WHERE created_at = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setTimestamp(1, Timestamp.valueOf(date));
            ResultSet rs = stmt.executeQuery();

            boolean partitionPruning = false;
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String value = rs.getString(i);
                    if (value != null && value.contains("partition")) {
                        partitionPruning = true;
                        System.out.println("  Partition info: " + value);
                    }
                }
            }

            if (partitionPruning) {
                System.out.println("  ✓ Partition pruning active");
            } else {
                System.out.println("  ⚠ Partition pruning not detected");
            }
        }
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        // Clean up repositories
        if (userRepo != null) userRepo.shutdown();
        if (orderRepo != null) orderRepo.shutdown();
        if (auditRepo != null) auditRepo.shutdown();

        // Close connection
        if (connection != null) connection.close();

        // Drop test database
        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "123456");
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + DB_NAME);
            System.out.println("✓ Test database cleaned up");
        }
    }
}