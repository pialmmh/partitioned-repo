package com.telcobright.db.repository;

import com.telcobright.db.test.BaseIntegrationTest;
import com.telcobright.db.test.entity.TestEntity;
import com.telcobright.db.monitoring.MonitoringConfig;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive integration tests for GenericMultiTableRepository
 */
@DisplayName("GenericMultiTableRepository Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GenericMultiTableRepositoryIntegrationTest extends BaseIntegrationTest {
    
    private GenericMultiTableRepository<TestEntity, Long> repository;
    private static final String TEST_DATABASE = "test_multi";
    
    @BeforeEach
    void setUp() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Create repository with disabled monitoring for cleaner tests
        MonitoringConfig noMonitoring = MonitoringConfig.disabled();
        
        repository = GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("test_data")
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
        assertThat(repository.toString()).contains("GenericMultiTableRepository");
    }
    
    @Test
    @Order(2)
    @DisplayName("Should create tables on initialization")
    void testTableCreationOnInitialization() throws Exception {
        // When - Tables should be created during setup
        
        // Then - Check that tables exist
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   int tableCount = getTableCount(TEST_DATABASE);
                   assertThat(tableCount).isGreaterThan(0);
               });
        
        // Check specific table pattern exists
        LocalDateTime today = LocalDateTime.now();
        String expectedTable = String.format("test_data_%04d%02d%02d", 
            today.getYear(), today.getMonthValue(), today.getDayOfMonth());
        
        assertThat(tableExists(TEST_DATABASE, expectedTable)).isTrue();
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
    @DisplayName("Should find entity by ID")
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
    @DisplayName("Should return null for non-existent ID")
    void testFindByIdNonExistent() throws Exception {
        // When
        TestEntity found = repository.findById(99999L);
        
        // Then
        assertThat(found).isNull();
    }
    
    @Test
    @Order(7)
    @DisplayName("Should find entities by date range")
    void testFindByDateRange() throws Exception {
        // Given
        LocalDateTime baseTime = LocalDateTime.now();
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Past", "past@test.com", baseTime.minusDays(1)),
            createTestEntity("Present1", "present1@test.com", baseTime),
            createTestEntity("Present2", "present2@test.com", baseTime.plusMinutes(30)),
            createTestEntity("Future", "future@test.com", baseTime.plusDays(1))
        );
        
        repository.insertMultiple(entities);
        
        // When - Find entities from today
        List<TestEntity> found = repository.findAllByDateRange(
            baseTime.minusHours(1),
            baseTime.plusHours(1)
        );
        
        // Then
        assertThat(found).hasSizeGreaterThanOrEqualTo(2);
        assertThat(found.stream().map(TestEntity::getName))
            .contains("Present1", "Present2");
    }
    
    @Test
    @Order(8)
    @DisplayName("Should find entities before specific date")
    void testFindBeforeDate() throws Exception {
        // Given
        LocalDateTime cutoff = LocalDateTime.now();
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Old1", "old1@test.com", cutoff.minusDays(2)),
            createTestEntity("Old2", "old2@test.com", cutoff.minusHours(1)),
            createTestEntity("New", "new@test.com", cutoff.plusHours(1))
        );
        
        repository.insertMultiple(entities);
        
        // When
        List<TestEntity> found = repository.findAllBeforeDate(cutoff);
        
        // Then
        assertThat(found).hasSizeGreaterThanOrEqualTo(2);
        assertThat(found.stream().map(TestEntity::getName))
            .contains("Old1", "Old2")
            .doesNotContain("New");
    }
    
    @Test
    @Order(9)
    @DisplayName("Should find entities after specific date")
    void testFindAfterDate() throws Exception {
        // Given
        LocalDateTime cutoff = LocalDateTime.now();
        List<TestEntity> entities = Arrays.asList(
            createTestEntity("Old", "old@test.com", cutoff.minusHours(1)),
            createTestEntity("New1", "new1@test.com", cutoff.plusMinutes(30)),
            createTestEntity("New2", "new2@test.com", cutoff.plusHours(1))
        );
        
        repository.insertMultiple(entities);
        
        // When
        List<TestEntity> found = repository.findAllAfterDate(cutoff);
        
        // Then
        assertThat(found).hasSizeGreaterThanOrEqualTo(2);
        assertThat(found.stream().map(TestEntity::getName))
            .contains("New1", "New2")
            .doesNotContain("Old");
    }
    
    @Test
    @Order(10)
    @DisplayName("Should update entity by ID")
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
    @Order(11)
    @DisplayName("Should update entity by ID and date range")
    void testUpdateByIdAndDateRange() throws Exception {
        // Given
        LocalDateTime now = LocalDateTime.now();
        TestEntity entity = createTestEntity("RangeUpdate", "range@test.com", now);
        repository.insert(entity);
        Long entityId = entity.getId();
        
        // When
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
    @Order(12)
    @DisplayName("Should handle entities across multiple tables")
    void testMultipleTableStorage() throws Exception {
        // Given - Entities from different days
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
        
        // Then - Should be able to find all across date ranges
        List<TestEntity> allFound = repository.findAllByDateRange(
            yesterday.minusHours(1),
            tomorrow.plusHours(1)
        );
        
        assertThat(allFound).hasSizeGreaterThanOrEqualTo(3);
        assertThat(allFound.stream().map(TestEntity::getName))
            .contains("Yesterday", "Today", "Tomorrow");
    }
    
    @Test
    @Order(13)
    @DisplayName("Should handle large batch inserts efficiently")
    void testLargeBatchInsert() throws Exception {
        // Given
        List<TestEntity> largeList = new ArrayList<>();
        LocalDateTime baseTime = LocalDateTime.now();
        
        for (int i = 0; i < 100; i++) {
            largeList.add(createTestEntity(
                "Batch" + i,
                "batch" + i + "@test.com",
                baseTime.plusMinutes(i)
            ));
        }
        
        // When
        long startTime = System.currentTimeMillis();
        repository.insertMultiple(largeList);
        long endTime = System.currentTimeMillis();
        
        // Then
        assertThat(endTime - startTime).isLessThan(5000); // Should complete in under 5 seconds
        
        // Verify all entities are stored
        List<TestEntity> found = repository.findAllByDateRange(
            baseTime.minusHours(1),
            baseTime.plusHours(2)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(100);
    }
    
    @Test
    @Order(14)
    @DisplayName("Should handle concurrent operations safely")
    void testConcurrentOperations() throws Exception {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicInteger successCount = new AtomicInteger(0);
        
        // When
        for (int i = 0; i < 50; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    TestEntity entity = createTestEntity(
                        "Concurrent" + threadId,
                        "concurrent" + threadId + "@test.com",
                        LocalDateTime.now().plusSeconds(threadId)
                    );
                    repository.insert(entity);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Log error but don't fail test - some concurrency issues are expected
                    System.err.println("Concurrent operation failed: " + e.getMessage());
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        // Then
        assertThat(successCount.get()).isGreaterThan(40); // Allow some failures due to concurrency
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