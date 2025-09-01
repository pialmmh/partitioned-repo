package com.telcobright.db.repository;

import com.telcobright.db.test.BaseIntegrationTest;
import com.telcobright.db.test.entity.TestEntity;
import com.telcobright.db.monitoring.MonitoringConfig;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive integration tests for GenericPartitionedTableRepository
 */
@DisplayName("GenericPartitionedTableRepository Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GenericPartitionedTableRepositoryIntegrationTest extends BaseIntegrationTest {
    
    private GenericPartitionedTableRepository<TestEntity, Long> repository;
    private static final String TEST_DATABASE = "test_partitioned";
    
    @BeforeEach
    void setUp() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Create repository with disabled monitoring for cleaner tests
        MonitoringConfig noMonitoring = MonitoringConfig.disabled();
        
        repository = GenericPartitionedTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tableName("test_data")
            .partitionRetentionPeriod(3)  // Small retention for testing
            .autoManagePartitions(false)  // Disable scheduler for tests
            .initializePartitionsOnStart(true)
            .monitoring(noMonitoring)
            .build();
    }
    
    @AfterEach
    void tearDown() {
        if (repository != null) {
            repository.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Should create repository successfully")
    void testRepositoryCreation() {
        // Then
        assertThat(repository).isNotNull();
        assertThat(repository.toString()).contains("GenericPartitionedTableRepository");
    }
    
    @Test
    @Order(2)
    @DisplayName("Should create partitioned table on initialization")
    void testPartitionedTableCreation() throws Exception {
        // When - Table should be created during setup
        
        // Then - Check that table exists
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertThat(tableExists(TEST_DATABASE, "test_data")).isTrue();
               });
        
        // Check that table is partitioned
        try (Connection conn = getConnection(TEST_DATABASE);
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS " +
                "WHERE TABLE_SCHEMA = '" + TEST_DATABASE + "' " +
                "AND TABLE_NAME = 'test_data' " +
                "AND PARTITION_NAME IS NOT NULL"
            );
            
            int partitionCount = 0;
            while (rs.next()) {
                partitionCount++;
                String partitionName = rs.getString("PARTITION_NAME");
                assertThat(partitionName).startsWith("p");
            }
            
            assertThat(partitionCount).isGreaterThan(0);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Should insert single entity successfully")
    void testInsertSingleEntity() throws Exception {
        // Given
        TestEntity entity = createTestEntity("John Doe", "john@test.com", LocalDateTime.now());
        
        // When
        repository.insert(entity);
        
        // Then
        assertThat(entity.getId()).isNotNull();
        assertThat(entity.getId()).isGreaterThan(0L);
        
        // Verify in database
        List<TestEntity> found = repository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo("John Doe");
        assertThat(found.get(0).getEmail()).isEqualTo("john@test.com");
    }
    
    @Test
    @Order(4)
    @DisplayName("Should insert multiple entities successfully")
    void testInsertMultipleEntities() throws Exception {
        // Given
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Alice", "alice@test.com", LocalDateTime.now()),
            createTestEntity("Bob", "bob@test.com", LocalDateTime.now().minusMinutes(30)),
            createTestEntity("Charlie", "charlie@test.com", LocalDateTime.now().plusMinutes(15))
        );
        
        // When
        repository.insertMultiple(entities);
        
        // Then
        entities.forEach(entity -> {
            assertThat(entity.getId()).isNotNull();
            assertThat(entity.getId()).isGreaterThan(0L);
        });
        
        // Verify all entities are in database
        List<TestEntity> found = repository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(3);
    }
    
    @Test
    @Order(5)
    @DisplayName("Should find entity by ID using partition scanning")
    void testFindById() throws Exception {
        // Given
        TestEntity entity = createTestEntity("FindMe", "findme@test.com", LocalDateTime.now());
        repository.insert(entity);
        Long entityId = entity.getId();
        
        // When
        TestEntity found = repository.findById(entityId);
        
        // Then
        assertThat(found).isNotNull();
        assertThat(found.getId()).isEqualTo(entityId);
        assertThat(found.getName()).isEqualTo("FindMe");
        assertThat(found.getEmail()).isEqualTo("findme@test.com");
    }
    
    @Test
    @Order(6)
    @DisplayName("Should utilize partition pruning for date range queries")
    void testPartitionPruning() throws Exception {
        // Given
        LocalDateTime baseTime = LocalDateTime.now();
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Past", "past@test.com", baseTime.minusDays(2)),
            createTestEntity("Present1", "present1@test.com", baseTime),
            createTestEntity("Present2", "present2@test.com", baseTime.plusMinutes(30)),
            createTestEntity("Future", "future@test.com", baseTime.plusDays(2))
        );
        
        repository.insertMultiple(entities);
        
        // When - Find entities from today only
        List<TestEntity> found = repository.findAllByDateRange(
            baseTime.minusHours(1),
            baseTime.plusHours(1)
        );
        
        // Then
        assertThat(found).hasSizeGreaterThanOrEqualTo(2);
        assertThat(found.stream().map(TestEntity::getName))
            .contains("Present1", "Present2")
            .doesNotContain("Past", "Future");
    }
    
    @Test
    @Order(7)
    @DisplayName("Should handle entities across multiple partitions")
    void testMultiplePartitionStorage() throws Exception {
        // Given - Entities from different days to hit different partitions
        LocalDateTime today = LocalDateTime.now();
        LocalDateTime yesterday = today.minusDays(1);
        LocalDateTime tomorrow = today.plusDays(1);
        
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Yesterday", "yesterday@test.com", yesterday),
            createTestEntity("Today", "today@test.com", today),
            createTestEntity("Tomorrow", "tomorrow@test.com", tomorrow)
        );
        
        // When
        repository.insertMultiple(entities);
        
        // Then - Should be able to find all across partitions
        List<TestEntity> allFound = repository.findAllByDateRange(
            yesterday.minusHours(1),
            tomorrow.plusHours(1)
        );
        
        assertThat(allFound).hasSizeGreaterThanOrEqualTo(3);
        assertThat(allFound.stream().map(TestEntity::getName))
            .contains("Yesterday", "Today", "Tomorrow");
        
        // Verify they are in different partitions by checking partition info
        try (Connection conn = getConnection(TEST_DATABASE);
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT id, name, created_at, " +
                "PARTITION_NAME() as partition_name " +
                "FROM test_data WHERE name IN ('Yesterday', 'Today', 'Tomorrow')"
            );
            
            List<String> partitionNames = new ArrayList<>();
            while (rs.next()) {
                String partitionName = rs.getString("partition_name");
                partitionNames.add(partitionName);
            }
            
            // Should have entities in multiple partitions
            assertThat(partitionNames.stream().distinct()).hasSizeGreaterThan(1);
        }
    }
    
    @Test
    @Order(8)
    @DisplayName("Should update entity by ID across partitions")
    void testUpdateById() throws Exception {
        // Given
        TestEntity entity = createTestEntity("Original", "original@test.com", LocalDateTime.now());
        repository.insert(entity);
        Long entityId = entity.getId();
        
        // When
        TestEntity updateData = new TestEntity();
        updateData.setName("Updated");
        updateData.setEmail("updated@test.com");
        updateData.setStatus("UPDATED");
        updateData.setUpdatedAt(LocalDateTime.now());
        
        repository.updateById(entityId, updateData);
        
        // Then
        TestEntity found = repository.findById(entityId);
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Updated");
        assertThat(found.getEmail()).isEqualTo("updated@test.com");
        assertThat(found.getStatus()).isEqualTo("UPDATED");
        assertThat(found.getUpdatedAt()).isNotNull();
    }
    
    @Test
    @Order(9)
    @DisplayName("Should update entity by ID with partition pruning")
    void testUpdateByIdAndDateRange() throws Exception {
        // Given
        LocalDateTime now = LocalDateTime.now();
        TestEntity entity = createTestEntity("RangeUpdate", "range@test.com", now);
        repository.insert(entity);
        Long entityId = entity.getId();
        
        // When - Update with date range to enable partition pruning
        TestEntity updateData = new TestEntity();
        updateData.setName("RangeUpdated");
        updateData.setStatus("RANGE_UPDATED");
        
        repository.updateByIdAndDateRange(entityId, updateData, 
            now.minusHours(1), now.plusHours(1));
        
        // Then
        TestEntity found = repository.findById(entityId);
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("RangeUpdated");
        assertThat(found.getStatus()).isEqualTo("RANGE_UPDATED");
    }
    
    @Test
    @Order(10)
    @DisplayName("Should handle primary key constraints correctly")
    void testPrimaryKeyConstraints() throws Exception {
        // Given
        TestEntity entity1 = createTestEntity("User1", "user1@test.com", LocalDateTime.now());
        TestEntity entity2 = createTestEntity("User2", "user2@test.com", LocalDateTime.now().plusMinutes(1));
        
        // When
        repository.insert(entity1);
        repository.insert(entity2);
        
        // Then
        assertThat(entity1.getId()).isNotEqualTo(entity2.getId());
        assertThat(entity1.getId()).isGreaterThan(0L);
        assertThat(entity2.getId()).isGreaterThan(0L);
    }
    
    @Test
    @Order(11)
    @DisplayName("Should handle large batch inserts efficiently with partitions")
    void testLargeBatchInsertWithPartitions() throws Exception {
        // Given
        List<TestEntity> largeList = new ArrayList<>();
        LocalDateTime baseTime = LocalDateTime.now();
        
        // Create entities across multiple days to hit different partitions
        for (int i = 0; i < 300; i++) {
            LocalDateTime entityTime = baseTime.plusHours(i / 100).plusMinutes(i % 60);
            largeList.add(createTestEntity(
                "Batch" + i,
                "batch" + i + "@test.com",
                entityTime
            ));
        }
        
        // When
        long startTime = System.currentTimeMillis();
        repository.insertMultiple(largeList);
        long endTime = System.currentTimeMillis();
        
        // Then
        assertThat(endTime - startTime).isLessThan(8000); // Should complete in under 8 seconds
        
        // Verify all entities are stored across partitions
        List<TestEntity> found = repository.findAllByDateRange(
            baseTime.minusHours(1),
            baseTime.plusDays(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(300);
    }
    
    @Test
    @Order(12)
    @DisplayName("Should verify partition-specific indexes are created")
    void testPartitionIndexes() throws Exception {
        // Given/When - Indexes should be created during table creation
        
        // Then - Check that indexes exist
        try (Connection conn = getConnection(TEST_DATABASE);
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE " +
                "FROM INFORMATION_SCHEMA.STATISTICS " +
                "WHERE TABLE_SCHEMA = '" + TEST_DATABASE + "' " +
                "AND TABLE_NAME = 'test_data' " +
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
            );
            
            List<String> indexNames = new ArrayList<>();
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                
                indexNames.add(indexName);
                
                // Verify expected indexes
                if (indexName.equals("idx_email_unique")) {
                    assertThat(nonUnique).isFalse(); // Should be unique
                    assertThat(columnName).isEqualTo("email");
                }
                if (indexName.equals("idx_name")) {
                    assertThat(columnName).isEqualTo("name");
                }
            }
            
            // Verify expected indexes are present
            assertThat(indexNames).contains("PRIMARY", "idx_name", "idx_email_unique", "idx_status_search");
        }
    }
    
    @Test
    @Order(13)
    @DisplayName("Should handle composite primary key for partitioned table")
    void testCompositePrimaryKey() throws Exception {
        // Given - Partitioned tables should have composite primary key (id, created_at)
        
        // Then - Check primary key structure
        try (Connection conn = getConnection(TEST_DATABASE);
             Statement stmt = conn.createStatement()) {
            
            ResultSet rs = stmt.executeQuery(
                "SELECT COLUMN_NAME, ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA = '" + TEST_DATABASE + "' " +
                "AND TABLE_NAME = 'test_data' " +
                "AND CONSTRAINT_NAME = 'PRIMARY' " +
                "ORDER BY ORDINAL_POSITION"
            );
            
            List<String> primaryKeyColumns = new ArrayList<>();
            while (rs.next()) {
                primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
            }
            
            // Should have composite primary key for partitioning
            assertThat(primaryKeyColumns).containsExactly("id", "created_at");
        }
    }
    
    @Test
    @Order(14)
    @DisplayName("Should demonstrate partition pruning performance benefits")
    void testPartitionPruningPerformance() throws Exception {
        // Given - Insert entities across multiple days
        LocalDateTime baseTime = LocalDateTime.now();
        List<TestEntity> entities = new ArrayList<>();
        
        for (int day = 0; day < 5; day++) {
            for (int i = 0; i < 50; i++) {
                entities.add(createTestEntity(
                    "Day" + day + "Entity" + i,
                    "day" + day + "entity" + i + "@test.com",
                    baseTime.plusDays(day).plusMinutes(i)
                ));
            }
        }
        
        repository.insertMultiple(entities);
        
        // When - Query specific day (should use partition pruning)
        long startTime = System.currentTimeMillis();
        List<TestEntity> oneDayResults = repository.findAllByDateRange(
            baseTime.withHour(0).withMinute(0).withSecond(0),
            baseTime.withHour(23).withMinute(59).withSecond(59)
        );
        long endTime = System.currentTimeMillis();
        
        // Then
        assertThat(oneDayResults).hasSizeGreaterThanOrEqualTo(50);
        assertThat(endTime - startTime).isLessThan(1000); // Should be fast due to partition pruning
        
        // Verify correct data returned
        assertThat(oneDayResults.stream().map(TestEntity::getName))
            .allMatch(name -> name.startsWith("Day0Entity"));
    }
    
    private TestEntity createTestEntity(String name, String email, LocalDateTime createdAt) {
        return new TestEntity(
            name,
            email,
            "ACTIVE",
            25,
            new BigDecimal("1000.00"),
            true,
            "Test description for " + name,
            createdAt
        );
    }
}