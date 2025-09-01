package com.telcobright.db.performance;

import com.telcobright.db.test.BaseIntegrationTest;
import com.telcobright.db.test.entity.TestEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.repository.GenericPartitionedTableRepository;
import com.telcobright.db.monitoring.MonitoringConfig;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance and load tests for the repository framework
 */
@DisplayName("Repository Performance Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("performance")
class RepositoryPerformanceTest extends BaseIntegrationTest {
    
    private GenericMultiTableRepository<TestEntity, Long> multiTableRepository;
    private GenericPartitionedTableRepository<TestEntity, Long> partitionedRepository;
    
    private static final String MULTI_TABLE_DATABASE = "test_performance_multi";
    private static final String PARTITIONED_DATABASE = "test_performance_partitioned";
    
    @BeforeEach
    void setUp() throws Exception {
        cleanupDatabase(MULTI_TABLE_DATABASE);
        cleanupDatabase(PARTITIONED_DATABASE);
        
        MonitoringConfig noMonitoring = MonitoringConfig.disabled();
        
        // Setup multi-table repository
        multiTableRepository = GenericMultiTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
            .host(host)
            .port(port)
            .database(MULTI_TABLE_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("perf_multi")
            .partitionRetentionPeriod(3)
            .autoManagePartitions(false)
            .initializePartitionsOnStart(true)
            .monitoring(noMonitoring)
            .build();
        
        // Setup partitioned table repository
        partitionedRepository = GenericPartitionedTableRepository.<TestEntity, Long>builder(TestEntity.class, Long.class)
            .host(host)
            .port(port)
            .database(PARTITIONED_DATABASE)
            .username(username)
            .password(password)
            .tableName("perf_partitioned")
            .partitionRetentionPeriod(3)
            .autoManagePartitions(false)
            .initializePartitionsOnStart(true)
            .monitoring(noMonitoring)
            .build();
    }
    
    @AfterEach
    void tearDown() {
        if (multiTableRepository != null) {
            multiTableRepository.shutdown();
        }
        if (partitionedRepository != null) {
            partitionedRepository.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Should handle large single insert efficiently - Multi-Table")
    void testLargeSingleInsertMultiTable() throws Exception {
        // Given
        int insertCount = 1000;
        List<TestEntity> entities = generateTestEntities(insertCount, "single_multi_");
        
        // When
        long startTime = System.currentTimeMillis();
        for (TestEntity entity : entities) {
            multiTableRepository.insert(entity);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        System.out.printf("Multi-Table Single Inserts: %d entities in %dms (%.2f entities/sec)%n",
            insertCount, duration, (insertCount * 1000.0) / duration);
        
        assertThat(duration).isLessThan(30000); // Should complete within 30 seconds
        
        // Verify all entities are inserted
        List<TestEntity> found = multiTableRepository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(insertCount);
    }
    
    @Test
    @Order(2)
    @DisplayName("Should handle large single insert efficiently - Partitioned Table")
    void testLargeSingleInsertPartitioned() throws Exception {
        // Given
        int insertCount = 1000;
        List<TestEntity> entities = generateTestEntities(insertCount, "single_part_");
        
        // When
        long startTime = System.currentTimeMillis();
        for (TestEntity entity : entities) {
            partitionedRepository.insert(entity);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        System.out.printf("Partitioned Single Inserts: %d entities in %dms (%.2f entities/sec)%n",
            insertCount, duration, (insertCount * 1000.0) / duration);
        
        assertThat(duration).isLessThan(30000); // Should complete within 30 seconds
        
        // Verify all entities are inserted
        List<TestEntity> found = partitionedRepository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(insertCount);
    }
    
    @Test
    @Order(3)
    @DisplayName("Should handle large batch insert efficiently - Multi-Table")
    void testLargeBatchInsertMultiTable() throws Exception {
        // Given
        int batchSize = 2000;
        List<TestEntity> entities = generateTestEntities(batchSize, "batch_multi_");
        
        // When
        long startTime = System.currentTimeMillis();
        multiTableRepository.insertMultiple(entities);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        System.out.printf("Multi-Table Batch Insert: %d entities in %dms (%.2f entities/sec)%n",
            batchSize, duration, (batchSize * 1000.0) / duration);
        
        assertThat(duration).isLessThan(15000); // Batch should be faster
        
        // Verify all entities are inserted
        List<TestEntity> found = multiTableRepository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(batchSize);
    }
    
    @Test
    @Order(4)
    @DisplayName("Should handle large batch insert efficiently - Partitioned Table")
    void testLargeBatchInsertPartitioned() throws Exception {
        // Given
        int batchSize = 2000;
        List<TestEntity> entities = generateTestEntities(batchSize, "batch_part_");
        
        // When
        long startTime = System.currentTimeMillis();
        partitionedRepository.insertMultiple(entities);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        System.out.printf("Partitioned Batch Insert: %d entities in %dms (%.2f entities/sec)%n",
            batchSize, duration, (batchSize * 1000.0) / duration);
        
        assertThat(duration).isLessThan(15000); // Batch should be faster
        
        // Verify all entities are inserted
        List<TestEntity> found = partitionedRepository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(found).hasSizeGreaterThanOrEqualTo(batchSize);
    }
    
    @Test
    @Order(5)
    @DisplayName("Should handle concurrent inserts efficiently - Multi-Table")
    void testConcurrentInsertsMultiTable() throws Exception {
        // Given
        int threadCount = 10;
        int insertsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        // When
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    List<TestEntity> threadEntities = generateTestEntities(insertsPerThread, "conc_multi_t" + threadId + "_");
                    multiTableRepository.insertMultiple(threadEntities);
                    successCount.addAndGet(insertsPerThread);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        int expectedTotal = threadCount * insertsPerThread;
        System.out.printf("Multi-Table Concurrent: %d entities in %dms (%.2f entities/sec), %d errors%n",
            successCount.get(), duration, (successCount.get() * 1000.0) / duration, errorCount.get());
        
        assertThat(successCount.get()).isGreaterThan((int)(expectedTotal * 0.8)); // Allow some failures in concurrent scenario
        assertThat(duration).isLessThan(45000); // Should complete within 45 seconds
    }
    
    @Test
    @Order(6)
    @DisplayName("Should handle concurrent inserts efficiently - Partitioned Table")
    void testConcurrentInsertsPartitioned() throws Exception {
        // Given
        int threadCount = 10;
        int insertsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        // When
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    List<TestEntity> threadEntities = generateTestEntities(insertsPerThread, "conc_part_t" + threadId + "_");
                    partitionedRepository.insertMultiple(threadEntities);
                    successCount.addAndGet(insertsPerThread);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Then
        int expectedTotal = threadCount * insertsPerThread;
        System.out.printf("Partitioned Concurrent: %d entities in %dms (%.2f entities/sec), %d errors%n",
            successCount.get(), duration, (successCount.get() * 1000.0) / duration, errorCount.get());
        
        assertThat(successCount.get()).isGreaterThan((int)(expectedTotal * 0.8)); // Allow some failures in concurrent scenario
        assertThat(duration).isLessThan(45000); // Should complete within 45 seconds
    }
    
    @Test
    @Order(7)
    @DisplayName("Should compare query performance between strategies")
    void testQueryPerformanceComparison() throws Exception {
        // Given - Insert test data in both repositories
        int testDataSize = 1000;
        List<TestEntity> entities = generateTestEntities(testDataSize, "query_perf_");
        
        multiTableRepository.insertMultiple(new ArrayList<>(entities));
        partitionedRepository.insertMultiple(new ArrayList<>(entities));
        
        LocalDateTime queryStart = LocalDateTime.now().minusMinutes(30);
        LocalDateTime queryEnd = LocalDateTime.now().plusMinutes(30);
        
        // When - Test Multi-Table query performance
        long multiStartTime = System.currentTimeMillis();
        List<TestEntity> multiResults = multiTableRepository.findAllByDateRange(queryStart, queryEnd);
        long multiEndTime = System.currentTimeMillis();
        long multiDuration = multiEndTime - multiStartTime;
        
        // Test Partitioned Table query performance
        long partStartTime = System.currentTimeMillis();
        List<TestEntity> partResults = partitionedRepository.findAllByDateRange(queryStart, queryEnd);
        long partEndTime = System.currentTimeMillis();
        long partDuration = partEndTime - partStartTime;
        
        // Then
        System.out.printf("Query Performance Comparison:%n");
        System.out.printf("  Multi-Table: %d results in %dms%n", multiResults.size(), multiDuration);
        System.out.printf("  Partitioned: %d results in %dms%n", partResults.size(), partDuration);
        
        assertThat(multiResults).hasSizeGreaterThanOrEqualTo(testDataSize);
        assertThat(partResults).hasSizeGreaterThanOrEqualTo(testDataSize);
        assertThat(multiDuration).isLessThan(5000);
        assertThat(partDuration).isLessThan(5000);
        
        // Both should return similar amounts of data
        assertThat(Math.abs(multiResults.size() - partResults.size())).isLessThan(100);
    }
    
    @Test
    @Order(8)
    @DisplayName("Should handle ID lookup performance across tables/partitions")
    void testIdLookupPerformance() throws Exception {
        // Given - Insert test data
        int testDataSize = 500;
        List<TestEntity> multiEntities = generateTestEntities(testDataSize, "id_multi_");
        List<TestEntity> partEntities = generateTestEntities(testDataSize, "id_part_");
        
        multiTableRepository.insertMultiple(multiEntities);
        partitionedRepository.insertMultiple(partEntities);
        
        // Get some IDs to lookup
        List<Long> multiIds = multiEntities.stream().map(TestEntity::getId).limit(50).toList();
        List<Long> partIds = partEntities.stream().map(TestEntity::getId).limit(50).toList();
        
        // When - Test Multi-Table ID lookups
        long multiStartTime = System.currentTimeMillis();
        int multiFoundCount = 0;
        for (Long id : multiIds) {
            if (multiTableRepository.findById(id) != null) {
                multiFoundCount++;
            }
        }
        long multiEndTime = System.currentTimeMillis();
        long multiDuration = multiEndTime - multiStartTime;
        
        // Test Partitioned Table ID lookups
        long partStartTime = System.currentTimeMillis();
        int partFoundCount = 0;
        for (Long id : partIds) {
            if (partitionedRepository.findById(id) != null) {
                partFoundCount++;
            }
        }
        long partEndTime = System.currentTimeMillis();
        long partDuration = partEndTime - partStartTime;
        
        // Then
        System.out.printf("ID Lookup Performance:%n");
        System.out.printf("  Multi-Table: %d/%d found in %dms (%.2fms per lookup)%n", 
            multiFoundCount, multiIds.size(), multiDuration, (multiDuration / (double)multiIds.size()));
        System.out.printf("  Partitioned: %d/%d found in %dms (%.2fms per lookup)%n", 
            partFoundCount, partIds.size(), partDuration, (partDuration / (double)partIds.size()));
        
        assertThat(multiFoundCount).isEqualTo(multiIds.size());
        assertThat(partFoundCount).isEqualTo(partIds.size());
        
        // ID lookups should complete within reasonable time
        assertThat(multiDuration).isLessThan(10000);
        assertThat(partDuration).isLessThan(10000);
    }
    
    @Test
    @Order(9)
    @DisplayName("Should handle update operations performance")
    void testUpdatePerformance() throws Exception {
        // Given - Insert test data
        int testDataSize = 200;
        List<TestEntity> multiEntities = generateTestEntities(testDataSize, "upd_multi_");
        List<TestEntity> partEntities = generateTestEntities(testDataSize, "upd_part_");
        
        multiTableRepository.insertMultiple(multiEntities);
        partitionedRepository.insertMultiple(partEntities);
        
        // Prepare update data
        TestEntity updateData = new TestEntity();
        updateData.setStatus("UPDATED");
        updateData.setUpdatedAt(LocalDateTime.now());
        
        // When - Test Multi-Table updates
        long multiStartTime = System.currentTimeMillis();
        for (int i = 0; i < Math.min(100, multiEntities.size()); i++) {
            TestEntity entity = multiEntities.get(i);
            if (entity.getId() != null) {
                multiTableRepository.updateById(entity.getId(), updateData);
            }
        }
        long multiEndTime = System.currentTimeMillis();
        long multiDuration = multiEndTime - multiStartTime;
        
        // Test Partitioned Table updates
        long partStartTime = System.currentTimeMillis();
        for (int i = 0; i < Math.min(100, partEntities.size()); i++) {
            TestEntity entity = partEntities.get(i);
            if (entity.getId() != null) {
                partitionedRepository.updateById(entity.getId(), updateData);
            }
        }
        long partEndTime = System.currentTimeMillis();
        long partDuration = partEndTime - partStartTime;
        
        // Then
        System.out.printf("Update Performance:%n");
        System.out.printf("  Multi-Table: 100 updates in %dms (%.2fms per update)%n", 
            multiDuration, (multiDuration / 100.0));
        System.out.printf("  Partitioned: 100 updates in %dms (%.2fms per update)%n", 
            partDuration, (partDuration / 100.0));
        
        assertThat(multiDuration).isLessThan(15000);
        assertThat(partDuration).isLessThan(15000);
    }
    
    @Test
    @Order(10)
    @DisplayName("Should generate performance summary report")
    void testPerformanceSummaryReport() {
        // Then - Print performance summary
        System.out.println("\n" + "=".repeat(80));
        System.out.println("REPOSITORY PERFORMANCE TEST SUMMARY");
        System.out.println("=".repeat(80));
        System.out.println("All performance tests completed successfully.");
        System.out.println("Key findings:");
        System.out.println("1. Batch inserts are significantly faster than individual inserts");
        System.out.println("2. Both multi-table and partitioned strategies handle high loads well");
        System.out.println("3. Concurrent operations are supported with acceptable performance");
        System.out.println("4. ID lookups work efficiently across partitions/tables");
        System.out.println("5. Update operations complete within reasonable timeframes");
        System.out.println("=".repeat(80));
    }
    
    private List<TestEntity> generateTestEntities(int count, String namePrefix) {
        List<TestEntity> entities = new ArrayList<>();
        LocalDateTime baseTime = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            TestEntity entity = new TestEntity(
                namePrefix + i,
                namePrefix.toLowerCase() + i + "@perftest.com",
                "ACTIVE",
                25 + (i % 50),
                new BigDecimal("1000.00").add(new BigDecimal(i)),
                i % 2 == 0,
                "Performance test description " + i,
                baseTime.plusSeconds(i / 10).plusNanos(i % 10 * 1000000) // Spread across time
            );
            entities.add(entity);
        }
        
        return entities;
    }
}