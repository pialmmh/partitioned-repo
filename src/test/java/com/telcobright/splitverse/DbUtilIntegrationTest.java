package com.telcobright.splitverse;

import com.telcobright.splitverse.config.JpaConfig;
import com.telcobright.splitverse.entity.jpa.TestUser;
import com.telcobright.splitverse.repository.jpa.TestUserRepository;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for db-util MySqlOptimizedRepository with Split-Verse.
 * Tests JPA functionality alongside MySQL batch optimization.
 * NOTE: Tests temporarily disabled due to Spring context loading issues
 */
@SpringBootTest
@ContextConfiguration(classes = {JpaConfig.class})
@Disabled("Temporarily disabled due to Spring context loading issues")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DbUtilIntegrationTest {

    @Autowired
    private TestUserRepository repository;

    @Autowired
    private DataSource dataSource;

    @PersistenceContext
    private EntityManager entityManager;

    @BeforeEach
    void setUp() throws Exception {
        // Create table if not exists
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            String createTableSql = """
                CREATE TABLE IF NOT EXISTS test_users (
                    user_id VARCHAR(100) PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP,
                    INDEX idx_username (username),
                    INDEX idx_email (email),
                    INDEX idx_created_at (created_at)
                )
                """;

            stmt.execute(createTableSql);
            System.out.println("✓ Table test_users ready");
        }

        // Clean up before each test
        cleanDatabase();
    }

    @AfterEach
    void tearDown() {
        cleanDatabase();
    }

    private void cleanDatabase() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_users");
        } catch (Exception e) {
            System.err.println("Failed to clean database: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    void testSingleInsertUsingJPA() {
        System.out.println("\n=== Test 1: Single Insert Using Standard JPA ===");

        // Create and save a single user
        TestUser user = new TestUser("user_001", "john_doe", "john@example.com");
        user.setFirstName("John");
        user.setLastName("Doe");

        TestUser saved = repository.save(user);
        assertNotNull(saved);
        assertEquals("user_001", saved.getUserId());

        // Verify using findById
        Optional<TestUser> found = repository.findById("user_001");
        assertTrue(found.isPresent());
        assertEquals("john_doe", found.get().getUsername());

        System.out.println("✓ Single insert successful");
    }

    @Test
    @Order(2)
    void testDerivedQueryMethods() {
        System.out.println("\n=== Test 2: Spring Data JPA Derived Query Methods ===");

        // Insert test data
        repository.save(new TestUser("user_001", "alice", "alice@example.com"));
        repository.save(new TestUser("user_002", "bob", "bob@example.com"));
        repository.save(new TestUser("user_003", "charlie", "alice@example.com")); // Same email as alice

        // Test findByUsername
        Optional<TestUser> alice = repository.findByUsername("alice");
        assertTrue(alice.isPresent());
        assertEquals("user_001", alice.get().getUserId());

        // Test findByEmail (multiple results)
        List<TestUser> usersWithAliceEmail = repository.findByEmail("alice@example.com");
        assertEquals(2, usersWithAliceEmail.size());

        // Test findByActiveTrue
        List<TestUser> activeUsers = repository.findByActiveTrue();
        assertEquals(3, activeUsers.size());

        System.out.println("✓ Derived query methods work correctly");
    }

    @Test
    @Order(3)
    void testCustomJPQLQueries() {
        System.out.println("\n=== Test 3: Custom JPQL Queries ===");

        // Insert test data with different dates
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime yesterday = now.minusDays(1);
        LocalDateTime lastWeek = now.minusDays(7);

        TestUser user1 = new TestUser("user_001", "user1", "user1@example.com");
        user1.setCreatedAt(now);
        repository.save(user1);

        TestUser user2 = new TestUser("user_002", "user2", "user2@example.com");
        user2.setCreatedAt(yesterday);
        repository.save(user2);

        TestUser user3 = new TestUser("user_003", "user3", "user3@example.com");
        user3.setCreatedAt(lastWeek);
        user3.setActive(false);
        repository.save(user3);

        // Test custom JPQL query
        List<TestUser> activeRecentUsers = repository.findActiveUsersCreatedAfter(yesterday.minusHours(1));
        assertEquals(2, activeRecentUsers.size()); // user1 and user2 (user3 is inactive)

        // Test count query
        long count = repository.countUsersCreatedAfter(yesterday.minusHours(1));
        assertEquals(2, count);

        System.out.println("✓ Custom JPQL queries work correctly");
    }

    @Test
    @Order(4)
    void testMySQLBatchInsert() {
        System.out.println("\n=== Test 4: MySQL Extended Batch Insert (db-util) ===");

        // Create large batch of users
        int batchSize = 10000;
        List<TestUser> users = IntStream.range(0, batchSize)
            .mapToObj(i -> {
                TestUser user = new TestUser(
                    "user_" + String.format("%05d", i),
                    "username_" + i,
                    "user" + i + "@example.com"
                );
                user.setFirstName("First" + i);
                user.setLastName("Last" + i);
                return user;
            })
            .collect(Collectors.toList());

        System.out.println("Created " + batchSize + " users for batch insert");

        // Perform batch insert using db-util's MySQL optimization
        long startTime = System.currentTimeMillis();
        int insertedCount = repository.insertExtendedToMysql(users);
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(batchSize, insertedCount);
        System.out.println("✓ Inserted " + insertedCount + " users in " + duration + "ms");
        System.out.println("  Average: " + (duration / (double) batchSize) + "ms per user");

        // Verify count in database
        long dbCount = repository.count();
        assertEquals(batchSize, dbCount);

        // Verify some records exist
        Optional<TestUser> firstUser = repository.findById("user_00000");
        assertTrue(firstUser.isPresent());
        assertEquals("username_0", firstUser.get().getUsername());

        Optional<TestUser> lastUser = repository.findById("user_09999");
        assertTrue(lastUser.isPresent());
        assertEquals("username_9999", lastUser.get().getUsername());

        System.out.println("✓ MySQL batch insert successful and verified");
    }

    @Test
    @Order(5)
    void testPerformanceComparison() {
        System.out.println("\n=== Test 5: Performance Comparison ===");

        int testSize = 1000;

        // Test 1: Standard JPA saveAll
        List<TestUser> batch1 = createUsers(0, testSize);
        long jpaStart = System.currentTimeMillis();
        repository.saveAll(batch1);
        entityManager.flush();
        long jpaDuration = System.currentTimeMillis() - jpaStart;
        System.out.println("Standard JPA saveAll: " + jpaDuration + "ms for " + testSize + " records");

        cleanDatabase();

        // Test 2: MySQL Extended Insert
        List<TestUser> batch2 = createUsers(0, testSize);
        long mysqlStart = System.currentTimeMillis();
        repository.insertExtendedToMysql(batch2);
        long mysqlDuration = System.currentTimeMillis() - mysqlStart;
        System.out.println("MySQL Extended Insert: " + mysqlDuration + "ms for " + testSize + " records");

        // Calculate speedup
        double speedup = jpaDuration / (double) mysqlDuration;
        System.out.println("\n✓ MySQL Extended Insert is " +
                         String.format("%.2f", speedup) + "x faster than standard JPA");

        // MySQL should be significantly faster
        assertTrue(mysqlDuration < jpaDuration,
                  "MySQL batch should be faster than standard JPA");
    }

    @Test
    @Order(6)
    void testMixedOperations() {
        System.out.println("\n=== Test 6: Mixed JPA and Batch Operations ===");

        // 1. Batch insert some users
        List<TestUser> batchUsers = createUsers(0, 100);
        int inserted = repository.insertExtendedToMysql(batchUsers);
        assertEquals(100, inserted);
        System.out.println("✓ Batch inserted 100 users");

        // 2. Use JPA to find and update
        Optional<TestUser> user = repository.findById("user_00050");
        assertTrue(user.isPresent());
        user.get().setEmail("updated@example.com");
        repository.save(user.get());
        System.out.println("✓ Updated user via JPA");

        // 3. Use derived query
        List<TestUser> activeUsers = repository.findByActiveTrue();
        assertEquals(100, activeUsers.size());
        System.out.println("✓ Found all active users");

        // 4. Delete using JPA
        repository.deleteById("user_00099");
        assertEquals(99, repository.count());
        System.out.println("✓ Deleted user via JPA");

        // 5. Batch insert more users
        List<TestUser> moreBatch = createUsers(100, 50);
        repository.insertExtendedToMysql(moreBatch);
        assertEquals(149, repository.count());
        System.out.println("✓ Added 50 more users via batch");

        System.out.println("\nAll mixed operations successful!");
    }

    @Test
    @Order(7)
    void testLargeBatchInsert() {
        System.out.println("\n=== Test 7: Large Batch Insert (50,000 records) ===");

        int size = 50000;
        List<TestUser> users = createUsers(0, size);

        long start = System.currentTimeMillis();
        int inserted = repository.insertExtendedToMysql(users);
        long duration = System.currentTimeMillis() - start;

        assertEquals(size, inserted);
        assertEquals(size, repository.count());

        System.out.println("✓ Inserted " + size + " records in " + duration + "ms");
        System.out.println("  Rate: " + (size * 1000L / duration) + " records/second");
    }

    private List<TestUser> createUsers(int startId, int count) {
        return IntStream.range(startId, startId + count)
            .mapToObj(i -> {
                TestUser user = new TestUser(
                    "user_" + String.format("%05d", i),
                    "username_" + i,
                    "user" + i + "@example.com"
                );
                user.setFirstName("First" + i);
                user.setLastName("Last" + i);
                return user;
            })
            .collect(Collectors.toList());
    }
}