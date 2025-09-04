package com.telcobright.tests;

import com.telcobright.api.RepositoryProxy;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.examples.entity.OrderEntity;
import com.telcobright.examples.entity.SmsEntity;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance and Stress Testing Suite
 */
public class PerformanceStressTest extends BaseRepositoryTest {
    
    private static final int WARM_UP_OPERATIONS = 1000;
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8, 16, 32};
    private static final int[] BATCH_SIZES = {1, 10, 100, 1000};
    
    private GenericMultiTableRepository<SmsEntity, Long> smsRepository;
    private GenericPartitionedTableRepository<OrderEntity, Long> orderRepository;
    
    @Override
    public void runAllTests() throws Exception {
        logger.info("Starting Performance and Stress Tests");
        
        setUp();
        initializeRepositories();
        
        try {
            // Warm-up
            performWarmUp();
            
            // Performance tests
            testInsertPerformance();
            testQueryPerformance();
            testConcurrentInserts();
            testConcurrentQueries();
            testMixedWorkload();
            testSustainedLoad();
            testConnectionPoolStress();
            testMemoryStress();
            testDiskIOStress();
            testFailureRecovery();
            
            logger.info("All Performance and Stress tests completed");
            
        } finally {
            if (smsRepository != null) smsRepository.shutdown();
            if (orderRepository != null) orderRepository.shutdown();
            tearDown();
        }
    }
    
    private void initializeRepositories() {
        smsRepository = GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
            .host(TEST_HOST)
            .port(TEST_PORT)
            .database(TEST_DATABASE)
            .username(TEST_USER)
            .password(TEST_PASSWORD)
            .tablePrefix("perf_sms")
            .partitionRetentionPeriod(7)
            .maxConnections(50)
            .build();
        
        orderRepository = GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
            .host(TEST_HOST)
            .port(TEST_PORT)
            .database(TEST_DATABASE)
            .username(TEST_USER)
            .password(TEST_PASSWORD)
            .tableName("perf_orders")
            .partitionRetentionDays(30)
            .maxConnections(50)
            .build();
        
        logger.info("Performance test repositories initialized");
    }
    
    /**
     * Warm-up phase
     */
    private void performWarmUp() throws Exception {
        logger.info("Performing warm-up with " + WARM_UP_OPERATIONS + " operations");
        
        for (int i = 0; i < WARM_UP_OPERATIONS; i++) {
            SmsEntity sms = createTestSms(i);
            smsRepository.insert(sms);
            
            if (i % 100 == 0) {
                // Some queries to warm up query path
                smsRepository.findById((long) (random.nextInt(i + 1)));
            }
        }
        
        // Clear metrics from warm-up
        totalOperations.set(0);
        successfulOperations.set(0);
        failedOperations.set(0);
        totalLatencyMs.set(0);
        latencies.clear();
        
        logger.info("Warm-up completed");
    }
    
    /**
     * Test 1: Insert Performance
     */
    private void testInsertPerformance() throws Exception {
        logger.info("TEST 1: Insert Performance Benchmarks");
        
        String testId = "insert_performance_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        Map<String, Map<Integer, Double>> results = new HashMap<>();
        
        // Test both repository types
        for (String repoType : Arrays.asList("MultiTable", "Partitioned")) {
            Map<Integer, Double> batchResults = new HashMap<>();
            
            for (int batchSize : BATCH_SIZES) {
                int totalRecords = 10000;
                int batches = totalRecords / batchSize;
                
                logger.info("Testing " + repoType + " with batch size " + batchSize);
                
                long startTime = System.currentTimeMillis();
                
                for (int batch = 0; batch < batches; batch++) {
                    if (repoType.equals("MultiTable")) {
                        List<SmsEntity> smsBatch = new ArrayList<>();
                        for (int i = 0; i < batchSize; i++) {
                            smsBatch.add(createTestSms(batch * batchSize + i));
                        }
                        
                        if (batchSize == 1) {
                            smsRepository.insert(smsBatch.get(0));
                        } else {
                            smsRepository.insertMultiple(smsBatch);
                        }
                    } else {
                        List<OrderEntity> orderBatch = new ArrayList<>();
                        for (int i = 0; i < batchSize; i++) {
                            orderBatch.add(createTestOrder(batch * batchSize + i));
                        }
                        
                        if (batchSize == 1) {
                            orderRepository.insert(orderBatch.get(0));
                        } else {
                            orderRepository.insertMultiple(orderBatch);
                        }
                    }
                }
                
                long duration = System.currentTimeMillis() - startTime;
                double tps = (totalRecords * 1000.0) / duration;
                
                batchResults.put(batchSize, tps);
                
                logger.info("  Batch size " + batchSize + ": " + 
                           String.format("%.0f TPS (%.2f ms/op)", tps, duration / (double)totalRecords));
                
                recordMetric(testId, repoType + "_batch_" + batchSize, tps);
            }
            
            results.put(repoType, batchResults);
        }
        
        // Print comparison
        logger.info("\nInsert Performance Summary:");
        logger.info("Batch Size | Multi-Table TPS | Partitioned TPS");
        logger.info("-----------|-----------------|----------------");
        for (int batchSize : BATCH_SIZES) {
            logger.info(String.format("%10d | %15.0f | %15.0f",
                batchSize,
                results.get("MultiTable").get(batchSize),
                results.get("Partitioned").get(batchSize)));
        }
        
        saveTestResults(testId, "Insert Performance", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 2: Query Performance
     */
    private void testQueryPerformance() throws Exception {
        logger.info("TEST 2: Query Performance Benchmarks");
        
        String testId = "query_performance_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Ensure data exists for queries
        prepareQueryTestData();
        
        Map<String, Long> queryMetrics = new LinkedHashMap<>();
        
        // Test various query types
        int iterations = 100;
        
        // 1. Primary Key Lookups
        long pkTotalTime = 0;
        for (int i = 0; i < iterations; i++) {
            long id = (long) (random.nextInt(10000) + 1);
            
            long start = System.nanoTime();
            OrderEntity order = orderRepository.findById(id);
            pkTotalTime += (System.nanoTime() - start) / 1_000_000;
        }
        queryMetrics.put("PK_LOOKUP", pkTotalTime / iterations);
        
        // 2. Date Range Queries - 1 Day
        long dayRangeTime = 0;
        for (int i = 0; i < iterations; i++) {
            LocalDateTime date = LocalDateTime.now().minusDays(random.nextInt(7));
            
            long start = System.nanoTime();
            List<OrderEntity> orders = orderRepository.findAllByDateRange(
                date.withHour(0).withMinute(0),
                date.withHour(23).withMinute(59)
            );
            dayRangeTime += (System.nanoTime() - start) / 1_000_000;
        }
        queryMetrics.put("DAY_RANGE", dayRangeTime / iterations);
        
        // 3. Date Range Queries - 7 Days
        long weekRangeTime = 0;
        for (int i = 0; i < iterations / 10; i++) { // Fewer iterations for longer queries
            LocalDateTime endDate = LocalDateTime.now();
            LocalDateTime startDate = endDate.minusDays(7);
            
            long start = System.nanoTime();
            List<OrderEntity> orders = orderRepository.findAllByDateRange(startDate, endDate);
            weekRangeTime += (System.nanoTime() - start) / 1_000_000;
        }
        queryMetrics.put("WEEK_RANGE", weekRangeTime / (iterations / 10));
        
        // 4. Cursor Pagination
        long cursorTime = 0;
        for (int i = 0; i < iterations; i++) {
            long cursor = (long) (random.nextInt(10000));
            
            long start = System.nanoTime();
            List<OrderEntity> batch = orderRepository.findBatchByIdGreaterThan(cursor, 100);
            cursorTime += (System.nanoTime() - start) / 1_000_000;
        }
        queryMetrics.put("CURSOR_BATCH", cursorTime / iterations);
        
        // Print results
        logger.info("\nQuery Performance Results:");
        logger.info("Query Type      | Avg Latency (ms) | Status");
        logger.info("----------------|------------------|--------");
        
        for (Map.Entry<String, Long> entry : queryMetrics.entrySet()) {
            String status = getPerformanceStatus(entry.getKey(), entry.getValue());
            logger.info(String.format("%-15s | %16d | %s", 
                entry.getKey(), entry.getValue(), status));
            
            recordMetric(testId, entry.getKey(), entry.getValue());
        }
        
        saveTestResults(testId, "Query Performance", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 3: Concurrent Inserts
     */
    private void testConcurrentInserts() throws Exception {
        logger.info("TEST 3: Concurrent Insert Scalability");
        
        String testId = "concurrent_inserts_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        Map<Integer, Double> scalabilityResults = new LinkedHashMap<>();
        
        for (int threadCount : THREAD_COUNTS) {
            logger.info("Testing with " + threadCount + " threads");
            
            final int recordsPerThread = 1000;
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch completeLatch = new CountDownLatch(threadCount);
            final AtomicLong totalTime = new AtomicLong(0);
            final AtomicInteger successCount = new AtomicInteger(0);
            
            List<Future<?>> futures = new ArrayList<>();
            
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                
                Future<?> future = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        
                        long threadStart = System.currentTimeMillis();
                        
                        for (int i = 0; i < recordsPerThread; i++) {
                            SmsEntity sms = createTestSms(threadId * recordsPerThread + i);
                            smsRepository.insert(sms);
                            successCount.incrementAndGet();
                        }
                        
                        long threadTime = System.currentTimeMillis() - threadStart;
                        totalTime.addAndGet(threadTime);
                        
                    } catch (Exception e) {
                        logger.error("Thread " + threadId + " failed: " + e.getMessage());
                    } finally {
                        completeLatch.countDown();
                    }
                });
                
                futures.add(future);
            }
            
            long start = System.currentTimeMillis();
            startLatch.countDown(); // Start all threads
            
            boolean completed = completeLatch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - start;
            
            if (!completed) {
                logger.warn("Timeout waiting for threads to complete");
                futures.forEach(f -> f.cancel(true));
            }
            
            double totalTPS = (successCount.get() * 1000.0) / duration;
            double tpsPerThread = totalTPS / threadCount;
            
            scalabilityResults.put(threadCount, totalTPS);
            
            logger.info("  Total TPS: " + String.format("%.0f", totalTPS));
            logger.info("  TPS/Thread: " + String.format("%.0f", tpsPerThread));
            logger.info("  Success Rate: " + 
                       String.format("%.2f%%", successCount.get() * 100.0 / (threadCount * recordsPerThread)));
            
            recordMetric(testId, "threads_" + threadCount, totalTPS);
            
            Thread.sleep(2000); // Brief pause between tests
        }
        
        // Analyze scalability
        logger.info("\nScalability Analysis:");
        double baseline = scalabilityResults.get(1);
        for (Map.Entry<Integer, Double> entry : scalabilityResults.entrySet()) {
            double scalingFactor = entry.getValue() / baseline;
            double efficiency = scalingFactor / entry.getKey() * 100;
            
            logger.info(String.format("Threads: %2d | TPS: %7.0f | Scaling: %.2fx | Efficiency: %.1f%%",
                entry.getKey(), entry.getValue(), scalingFactor, efficiency));
        }
        
        saveTestResults(testId, "Concurrent Inserts", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 4: Concurrent Queries
     */
    private void testConcurrentQueries() throws Exception {
        logger.info("TEST 4: Concurrent Query Load");
        
        String testId = "concurrent_queries_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Ensure sufficient data
        prepareQueryTestData();
        
        final int threadCount = 10;
        final int duration = 30; // seconds
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger queryCount = new AtomicInteger(0);
        final AtomicLong queryTime = new AtomicLong(0);
        
        List<Future<?>> futures = new ArrayList<>();
        
        // Different query types for different threads
        String[] queryTypes = {"PK", "PK", "PK", "PK", "RANGE", "RANGE", "RANGE", "CURSOR", "CURSOR", "COMPLEX"};
        
        for (int t = 0; t < threadCount; t++) {
            final String queryType = queryTypes[t];
            
            Future<?> future = executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    long endTime = System.currentTimeMillis() + (duration * 1000);
                    
                    while (System.currentTimeMillis() < endTime) {
                        long start = System.nanoTime();
                        
                        switch (queryType) {
                            case "PK":
                                orderRepository.findById((long) (random.nextInt(10000) + 1));
                                break;
                                
                            case "RANGE":
                                LocalDateTime date = LocalDateTime.now().minusDays(random.nextInt(7));
                                orderRepository.findAllByDateRange(
                                    date.withHour(0).withMinute(0),
                                    date.withHour(23).withMinute(59)
                                );
                                break;
                                
                            case "CURSOR":
                                orderRepository.findBatchByIdGreaterThan(
                                    (long) random.nextInt(10000), 
                                    100
                                );
                                break;
                                
                            case "COMPLEX":
                                // Complex query combining multiple conditions
                                LocalDateTime start2 = LocalDateTime.now().minusDays(30);
                                LocalDateTime end2 = LocalDateTime.now();
                                orderRepository.findAllByDateRange(start2, end2);
                                break;
                        }
                        
                        long elapsed = (System.nanoTime() - start) / 1_000_000;
                        queryTime.addAndGet(elapsed);
                        queryCount.incrementAndGet();
                        
                        Thread.sleep(10); // Small delay between queries
                    }
                    
                } catch (Exception e) {
                    logger.error("Query thread failed: " + e.getMessage());
                }
            });
            
            futures.add(future);
        }
        
        logger.info("Running " + threadCount + " concurrent query threads for " + duration + " seconds");
        startLatch.countDown();
        
        // Wait for completion
        for (Future<?> future : futures) {
            try {
                future.get(duration + 5, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
            }
        }
        
        double avgLatency = queryCount.get() > 0 ? 
            queryTime.get() / (double) queryCount.get() : 0;
        double qps = queryCount.get() / (double) duration;
        
        logger.info("Concurrent Query Results:");
        logger.info("  Total Queries: " + queryCount.get());
        logger.info("  Queries/Second: " + String.format("%.0f", qps));
        logger.info("  Avg Latency: " + String.format("%.2f ms", avgLatency));
        
        recordMetric(testId, "queries_per_second", qps);
        recordMetric(testId, "avg_latency_ms", avgLatency);
        
        saveTestResults(testId, "Concurrent Queries", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 5: Mixed Workload
     */
    private void testMixedWorkload() throws Exception {
        logger.info("TEST 5: Mixed Read/Write Workload");
        
        String testId = "mixed_workload_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        final int duration = 60; // seconds
        final AtomicInteger insertCount = new AtomicInteger(0);
        final AtomicInteger queryCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicLong totalLatency = new AtomicLong(0);
        
        // Start monitoring thread
        ScheduledFuture<?> monitor = scheduledExecutor.scheduleAtFixedRate(() -> {
            logger.info(String.format("Progress - Inserts: %d, Queries: %d, Updates: %d",
                insertCount.get(), queryCount.get(), updateCount.get()));
        }, 10, 10, TimeUnit.SECONDS);
        
        // Workload distribution: 40% inserts, 40% queries, 20% updates
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int t = 0; t < threadCount; t++) {
            final int threadType = t % 5; // 0-1: insert, 2-3: query, 4: update
            
            executorService.submit(() -> {
                try {
                    long endTime = System.currentTimeMillis() + (duration * 1000);
                    
                    while (System.currentTimeMillis() < endTime) {
                        long start = System.nanoTime();
                        
                        if (threadType < 2) {
                            // Insert
                            SmsEntity sms = createTestSms(insertCount.get());
                            smsRepository.insert(sms);
                            insertCount.incrementAndGet();
                            
                        } else if (threadType < 4) {
                            // Query
                            LocalDateTime date = LocalDateTime.now().minusDays(random.nextInt(7));
                            smsRepository.findAllByDateRange(
                                date.withHour(0).withMinute(0),
                                date.withHour(23).withMinute(59)
                            );
                            queryCount.incrementAndGet();
                            
                        } else {
                            // Update
                            Long id = (long) (random.nextInt(insertCount.get() + 1) + 1);
                            SmsEntity sms = smsRepository.findById(id);
                            if (sms != null) {
                                sms.setStatus("updated");
                                smsRepository.updateById(id, sms);
                                updateCount.incrementAndGet();
                            }
                        }
                        
                        long elapsed = (System.nanoTime() - start) / 1_000_000;
                        totalLatency.addAndGet(elapsed);
                        
                        Thread.sleep(random.nextInt(50)); // Variable delay
                    }
                } catch (Exception e) {
                    logger.error("Mixed workload thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Wait for completion
        boolean completed = latch.await(duration + 10, TimeUnit.SECONDS);
        monitor.cancel(false);
        
        int totalOps = insertCount.get() + queryCount.get() + updateCount.get();
        double avgLatency = totalOps > 0 ? totalLatency.get() / (double) totalOps : 0;
        double opsPerSec = totalOps / (double) duration;
        
        logger.info("\nMixed Workload Results:");
        logger.info("  Total Operations: " + totalOps);
        logger.info("  Inserts: " + insertCount.get());
        logger.info("  Queries: " + queryCount.get());
        logger.info("  Updates: " + updateCount.get());
        logger.info("  Ops/Second: " + String.format("%.0f", opsPerSec));
        logger.info("  Avg Latency: " + String.format("%.2f ms", avgLatency));
        
        saveTestResults(testId, "Mixed Workload", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 6: Sustained Load
     */
    private void testSustainedLoad() throws Exception {
        logger.info("TEST 6: Sustained Load Test (5 minutes at 500 TPS)");
        
        String testId = "sustained_load_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        final int targetTPS = 500;
        final int duration = 300; // 5 minutes
        final int threadCount = 10;
        final int opsPerThreadPerSecond = targetTPS / threadCount;
        
        final AtomicInteger completedOps = new AtomicInteger(0);
        final AtomicLong totalLatency = new AtomicLong(0);
        final List<Long> secondlyTPS = Collections.synchronizedList(new ArrayList<>());
        
        // Monitor thread
        ScheduledFuture<?> monitor = scheduledExecutor.scheduleAtFixedRate(() -> {
            int ops = completedOps.getAndSet(0);
            secondlyTPS.add((long) ops);
            logger.info("Current TPS: " + ops);
        }, 1, 1, TimeUnit.SECONDS);
        
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int t = 0; t < threadCount; t++) {
            executorService.submit(() -> {
                try {
                    long endTime = System.currentTimeMillis() + (duration * 1000);
                    
                    while (System.currentTimeMillis() < endTime) {
                        long secondStart = System.currentTimeMillis();
                        
                        for (int op = 0; op < opsPerThreadPerSecond; op++) {
                            long start = System.nanoTime();
                            
                            OrderEntity order = createTestOrder(completedOps.get());
                            orderRepository.insert(order);
                            
                            long elapsed = (System.nanoTime() - start) / 1_000_000;
                            totalLatency.addAndGet(elapsed);
                            completedOps.incrementAndGet();
                        }
                        
                        // Sleep to maintain target rate
                        long elapsed = System.currentTimeMillis() - secondStart;
                        if (elapsed < 1000) {
                            Thread.sleep(1000 - elapsed);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Sustained load thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        boolean completed = latch.await(duration + 10, TimeUnit.SECONDS);
        monitor.cancel(false);
        
        // Analyze results
        long totalOps = secondlyTPS.stream().mapToLong(Long::longValue).sum();
        double avgTPS = totalOps / (double) duration;
        double avgLatency = totalOps > 0 ? totalLatency.get() / (double) totalOps : 0;
        
        // Calculate TPS stability
        double tpsStdDev = calculateStandardDeviation(secondlyTPS);
        double tpsVariation = (tpsStdDev / avgTPS) * 100;
        
        logger.info("\nSustained Load Results:");
        logger.info("  Duration: " + duration + " seconds");
        logger.info("  Target TPS: " + targetTPS);
        logger.info("  Actual Avg TPS: " + String.format("%.0f", avgTPS));
        logger.info("  TPS Std Dev: " + String.format("%.1f", tpsStdDev));
        logger.info("  TPS Variation: " + String.format("%.1f%%", tpsVariation));
        logger.info("  Avg Latency: " + String.format("%.2f ms", avgLatency));
        
        String status = avgTPS >= targetTPS * 0.95 ? "PASS" : "FAIL";
        saveTestResults(testId, "Sustained Load", testStart, System.currentTimeMillis(), status);
    }
    
    /**
     * Test 7: Connection Pool Stress
     */
    private void testConnectionPoolStress() throws Exception {
        logger.info("TEST 7: Connection Pool Exhaustion Test");
        
        String testId = "connection_pool_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Try to exhaust connection pool
        int maxConnections = 50;
        int threadCount = 100; // More threads than connections
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger timeoutCount = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    // Try to hold connection
                    OrderEntity order = createTestOrder(random.nextInt(10000));
                    orderRepository.insert(order);
                    
                    // Simulate work
                    Thread.sleep(random.nextInt(1000));
                    
                    // More operations
                    orderRepository.findById(order.getId());
                    
                    successCount.incrementAndGet();
                    
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("timeout")) {
                        timeoutCount.incrementAndGet();
                    }
                } finally {
                    completeLatch.countDown();
                }
            });
        }
        
        logger.info("Starting " + threadCount + " threads with " + maxConnections + " max connections");
        startLatch.countDown();
        
        boolean completed = completeLatch.await(30, TimeUnit.SECONDS);
        
        logger.info("Connection Pool Stress Results:");
        logger.info("  Threads: " + threadCount);
        logger.info("  Max Connections: " + maxConnections);
        logger.info("  Successful: " + successCount.get());
        logger.info("  Timeouts: " + timeoutCount.get());
        logger.info("  Success Rate: " + 
                   String.format("%.1f%%", successCount.get() * 100.0 / threadCount));
        
        saveTestResults(testId, "Connection Pool Stress", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 8: Memory Stress
     */
    private void testMemoryStress() throws Exception {
        logger.info("TEST 8: Memory Stress Test");
        
        String testId = "memory_stress_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Get initial memory
        Runtime runtime = Runtime.getRuntime();
        System.gc();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        logger.info("Initial memory usage: " + (initialMemory / 1024 / 1024) + " MB");
        
        // Create large result sets
        List<List<OrderEntity>> largeSets = new ArrayList<>();
        
        try {
            for (int i = 0; i < 10; i++) {
                // Fetch large batch
                List<OrderEntity> batch = orderRepository.findAllByDateRange(
                    LocalDateTime.now().minusDays(30),
                    LocalDateTime.now()
                );
                
                largeSets.add(batch);
                
                long currentMemory = runtime.totalMemory() - runtime.freeMemory();
                logger.info("Iteration " + (i + 1) + " - Memory: " + 
                           (currentMemory / 1024 / 1024) + " MB, " +
                           "Batch size: " + batch.size());
                
                if (currentMemory > initialMemory * 10) {
                    logger.warn("Memory usage increased 10x, stopping test");
                    break;
                }
            }
            
            // Hold references to prevent GC
            Thread.sleep(5000);
            
        } finally {
            // Clear references
            largeSets.clear();
            System.gc();
            Thread.sleep(2000);
            
            long finalMemory = runtime.totalMemory() - runtime.freeMemory();
            logger.info("Final memory usage: " + (finalMemory / 1024 / 1024) + " MB");
            
            // Check for memory leak
            if (finalMemory > initialMemory * 2) {
                logger.warn("Potential memory leak detected");
            }
        }
        
        saveTestResults(testId, "Memory Stress", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 9: Disk I/O Stress
     */
    private void testDiskIOStress() throws Exception {
        logger.info("TEST 9: Disk I/O Stress Test");
        
        String testId = "disk_io_stress_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Force disk writes by disabling caching
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1");
        }
        
        int batchCount = 100;
        int batchSize = 1000;
        
        logger.info("Writing " + (batchCount * batchSize) + " records with immediate flush");
        
        for (int batch = 0; batch < batchCount; batch++) {
            List<OrderEntity> orders = new ArrayList<>();
            
            for (int i = 0; i < batchSize; i++) {
                orders.add(createTestOrder(batch * batchSize + i));
            }
            
            long batchStart = System.currentTimeMillis();
            orderRepository.insertMultiple(orders);
            long batchTime = System.currentTimeMillis() - batchStart;
            
            if (batch % 10 == 0) {
                logger.info("Batch " + batch + ": " + batchTime + "ms");
                
                // Check table size
                try (Connection conn = connectionProvider.getConnection();
                     Statement stmt = conn.createStatement()) {
                    
                    ResultSet rs = stmt.executeQuery(
                        "SELECT " +
                        "  (data_length + index_length)/1024/1024 as size_mb " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = '" + TEST_DATABASE + "' " +
                        "AND table_name = 'perf_orders'"
                    );
                    
                    if (rs.next()) {
                        logger.info("  Table size: " + 
                                   String.format("%.2f MB", rs.getDouble("size_mb")));
                    }
                }
            }
        }
        
        // Restore settings
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 2");
        }
        
        saveTestResults(testId, "Disk I/O Stress", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    /**
     * Test 10: Failure Recovery
     */
    private void testFailureRecovery() throws Exception {
        logger.info("TEST 10: Failure and Recovery Test");
        
        String testId = "failure_recovery_" + System.currentTimeMillis();
        long testStart = System.currentTimeMillis();
        
        // Test connection failure recovery
        logger.info("Simulating connection failures...");
        
        int operations = 100;
        int failures = 0;
        int recoveries = 0;
        
        for (int i = 0; i < operations; i++) {
            try {
                if (i == 50) {
                    // Simulate connection issue by maxing out connections
                    logger.info("Inducing connection stress at operation " + i);
                    
                    List<Connection> connections = new ArrayList<>();
                    try {
                        for (int c = 0; c < 100; c++) {
                            connections.add(connectionProvider.getConnection());
                        }
                    } catch (SQLException e) {
                        // Expected
                        failures++;
                    } finally {
                        for (Connection conn : connections) {
                            try { conn.close(); } catch (Exception ignored) {}
                        }
                    }
                }
                
                // Normal operation
                OrderEntity order = createTestOrder(i);
                orderRepository.insert(order);
                
                if (failures > 0 && i > 50) {
                    recoveries++;
                }
                
            } catch (Exception e) {
                failures++;
                logger.debug("Operation " + i + " failed: " + e.getMessage());
                
                // Wait and retry
                Thread.sleep(100);
            }
        }
        
        logger.info("Failure Recovery Results:");
        logger.info("  Total Operations: " + operations);
        logger.info("  Failures: " + failures);
        logger.info("  Recoveries: " + recoveries);
        logger.info("  Final Success Rate: " + 
                   String.format("%.1f%%", (operations - failures) * 100.0 / operations));
        
        saveTestResults(testId, "Failure Recovery", testStart, System.currentTimeMillis(), "COMPLETE");
    }
    
    // Helper methods
    
    private SmsEntity createTestSms(int id) {
        SmsEntity sms = new SmsEntity();
        sms.setSenderId("sender_" + id);
        sms.setReceiverId("receiver_" + id);
        sms.setMessage("Performance test message " + id);
        sms.setStatus("sent");
        sms.setCreatedAt(LocalDateTime.now().minusMinutes(random.nextInt(1440)));
        return sms;
    }
    
    private OrderEntity createTestOrder(int id) {
        OrderEntity order = new OrderEntity();
        order.setCustomerId(1000L + id);
        order.setAmount(BigDecimal.valueOf(random.nextDouble() * 1000));
        order.setStatus("pending");
        order.setCreatedAt(LocalDateTime.now().minusMinutes(random.nextInt(1440)));
        return order;
    }
    
    private void prepareQueryTestData() throws SQLException {
        // Ensure we have data for queries
        for (int i = 0; i < 10000; i++) {
            OrderEntity order = createTestOrder(i);
            orderRepository.insert(order);
        }
    }
    
    private String getPerformanceStatus(String queryType, long latency) {
        Map<String, Long> thresholds = new HashMap<>();
        thresholds.put("PK_LOOKUP", 10L);
        thresholds.put("DAY_RANGE", 50L);
        thresholds.put("WEEK_RANGE", 200L);
        thresholds.put("CURSOR_BATCH", 50L);
        
        Long threshold = thresholds.getOrDefault(queryType, 100L);
        
        if (latency <= threshold) {
            return "✅ PASS";
        } else if (latency <= threshold * 2) {
            return "⚠️ WARN";
        } else {
            return "❌ FAIL";
        }
    }
    
    private double calculateStandardDeviation(List<Long> values) {
        if (values.isEmpty()) return 0;
        
        double mean = values.stream().mapToLong(Long::longValue).average().orElse(0);
        double variance = values.stream()
            .mapToDouble(v -> Math.pow(v - mean, 2))
            .average()
            .orElse(0);
        
        return Math.sqrt(variance);
    }
}