package com.telcobright.splitverse.tests;

import com.telcobright.api.ShardingRepository;
import com.telcobright.core.config.DataSourceConfig;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.enums.ShardingStrategy;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.RepositoryMode;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for CRUD operations with the new API.
 * Requires MySQL running at 127.0.0.1:3306 with root/123456
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrudOperationsTest {

    public static class User implements ShardingEntity {
        private String id;
        private LocalDateTime createdAt;
        private String name;
        private String email;
        private Integer age;

        @Override
        public String getId() { return id; }
        @Override
        public void setId(String id) { this.id = id; }
        @Override
        public LocalDateTime getCreatedAt() { return createdAt; }
        @Override
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public Integer getAge() { return age; }
        public void setAge(Integer age) { this.age = age; }
    }

    private static ShardingRepository<User> repository;
    private static String testUserId;

    @BeforeAll
    public static void setUpRepository() {
        // Create test database if not exists
        try {
            createTestDatabase();
        } catch (Exception e) {
            System.err.println("Failed to create test database: " + e.getMessage());
        }

        // Initialize repository with new API
        repository = SplitVerseRepository.<User>builder()
            .withEntityClass(User.class)
            .withTableName("users")
            .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
            .withPartitionColumn("created_at", PartitionColumnType.LOCAL_DATE_TIME)
            .withPartitionRange(PartitionRange.DAILY)
            .withRepositoryMode(RepositoryMode.MULTI_TABLE)
            .withRetentionDays(7)
            .withIdSize(22)
            .withDataSources(Arrays.asList(
                DataSourceConfig.create("127.0.0.1", 3306, "test_split_verse", "root", "123456")
            ))
            .build();
    }

    private static void createTestDatabase() throws Exception {
        // Create database using mysql command
        String command = "mysql -h 127.0.0.1 -P 3306 -u root -p123456 -e \"CREATE DATABASE IF NOT EXISTS test_split_verse\"";
        Process process = Runtime.getRuntime().exec(command);
        process.waitFor();
    }

    @AfterAll
    public static void tearDown() {
        if (repository != null) {
            repository.shutdown();
        }
    }

    @Test
    @Order(1)
    public void testInsertSingleEntity() throws SQLException {
        User user = new User();
        testUserId = UUID.randomUUID().toString().replace("-", "").substring(0, 22);
        user.setId(testUserId);
        user.setCreatedAt(LocalDateTime.now());
        user.setName("John Doe");
        user.setEmail("john@example.com");
        user.setAge(30);

        assertDoesNotThrow(() -> repository.insert(user));
        System.out.println("Inserted user with ID: " + testUserId);
    }

    @Test
    @Order(2)
    public void testFindById() throws SQLException {
        User found = repository.findById(testUserId);
        assertNotNull(found, "User should be found by ID");
        assertEquals("John Doe", found.getName());
        assertEquals("john@example.com", found.getEmail());
        assertEquals(30, found.getAge());
    }

    @Test
    @Order(3)
    public void testInsertMultipleEntities() throws SQLException {
        List<User> users = Arrays.asList(
            createUser("Alice", "alice@example.com", 25),
            createUser("Bob", "bob@example.com", 35),
            createUser("Charlie", "charlie@example.com", 28)
        );

        assertDoesNotThrow(() -> repository.insertMultiple(users));
        System.out.println("Inserted " + users.size() + " users");
    }

    @Test
    @Order(4)
    public void testFindAllByDateRange() throws SQLException {
        LocalDateTime start = LocalDateTime.now().minusHours(1);
        LocalDateTime end = LocalDateTime.now().plusHours(1);

        List<User> users = repository.findAllByDateRange(start, end);
        assertNotNull(users);
        assertTrue(users.size() >= 4, "Should find at least 4 users in date range");
    }

    @Test
    @Order(5)
    public void testUpdateById() throws SQLException {
        User updated = new User();
        updated.setId(testUserId);
        updated.setCreatedAt(LocalDateTime.now());
        updated.setName("John Updated");
        updated.setEmail("john.updated@example.com");
        updated.setAge(31);

        assertDoesNotThrow(() -> repository.updateById(testUserId, updated));

        User found = repository.findById(testUserId);
        assertEquals("John Updated", found.getName());
        assertEquals("john.updated@example.com", found.getEmail());
        assertEquals(31, found.getAge());
    }

    @Test
    @Order(6)
    public void testFindBatchByIdGreaterThan() throws SQLException {
        List<User> batch = repository.findBatchByIdGreaterThan("", 2);
        assertNotNull(batch);
        assertTrue(batch.size() <= 2, "Batch size should be limited to 2");

        if (!batch.isEmpty()) {
            String lastId = batch.get(batch.size() - 1).getId();
            List<User> nextBatch = repository.findBatchByIdGreaterThan(lastId, 2);
            assertNotNull(nextBatch);

            // Verify cursor-based pagination works correctly
            if (!nextBatch.isEmpty()) {
                assertTrue(nextBatch.get(0).getId().compareTo(lastId) > 0,
                    "Next batch should start after last ID");
            }
        }
    }

    @Test
    @Order(7)
    public void testFindOneByIdGreaterThan() throws SQLException {
        User first = repository.findOneByIdGreaterThan("");
        if (first != null) {
            User second = repository.findOneByIdGreaterThan(first.getId());
            if (second != null) {
                assertTrue(second.getId().compareTo(first.getId()) > 0,
                    "Second user ID should be greater than first");
            }
        }
    }

    @Test
    @Order(8)
    public void testFindAllBeforeDate() throws SQLException {
        LocalDateTime cutoff = LocalDateTime.now().plusMinutes(1);
        List<User> users = repository.findAllBeforeDate(cutoff);
        assertNotNull(users);
        assertTrue(users.size() >= 4, "Should find users created before cutoff");
    }

    @Test
    @Order(9)
    public void testFindAllAfterDate() throws SQLException {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(1);
        List<User> users = repository.findAllAfterDate(cutoff);
        assertNotNull(users);
        assertTrue(users.size() >= 4, "Should find users created after cutoff");
    }

    private User createUser(String name, String email, int age) {
        User user = new User();
        user.setId(UUID.randomUUID().toString().replace("-", "").substring(0, 22));
        user.setCreatedAt(LocalDateTime.now());
        user.setName(name);
        user.setEmail(email);
        user.setAge(age);
        return user;
    }
}