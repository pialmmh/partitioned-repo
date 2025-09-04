package com.telcobright.tests;

import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.examples.entity.OrderEntity;
import com.telcobright.examples.entity.SmsEntity;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Concurrent Operations Test Suite
 */
public class ConcurrentOperationsTest extends BaseRepositoryTest {
    
    private GenericMultiTableRepository<SmsEntity, Long> smsRepository;
    private GenericPartitionedTableRepository<OrderEntity, Long> orderRepository;
    
    @Override
    public void runAllTests() throws Exception {
        logger.info("Starting Concurrent Operations Tests");
        
        setUp();
        initializeRepositories();
        
        try {
            testConcurrentInserts();
            testConcurrentReadsWrites();
            testConcurrentUpdates();
            testDeadlockHandling();
            testRaceConditions();
            
            logger.info("All Concurrent Operations tests completed");
            
        } finally {
            if (smsRepository != null) smsRepository.shutdown();
            if (orderRepository != null) orderRepository.shutdown();
            tearDown();
        }
    }
    
    private void initializeRepositories() {
        smsRepository = GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
            .host(TEST_HOST).port(TEST_PORT).database(TEST_DATABASE)
            .username(TEST_USER).password(TEST_PASSWORD)
            .tablePrefix("concurrent_sms")
            .partitionRetentionPeriod(7)
            .build();
        
        orderRepository = GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
            .host(TEST_HOST).port(TEST_PORT).database(TEST_DATABASE)
            .username(TEST_USER).password(TEST_PASSWORD)
            .tableName("concurrent_orders")
            .partitionRetentionDays(30)
            .build();
    }
    
    private void testConcurrentInserts() throws Exception {
        logger.info("TEST: Concurrent Insert Operations");
        
        int threadCount = 10;
        int recordsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        Set<Long> allIds = Collections.synchronizedSet(new HashSet<>());
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < recordsPerThread; i++) {
                        OrderEntity order = new OrderEntity();
                        order.setCustomerId(1000L + threadId * 1000 + i);
                        order.setAmount(BigDecimal.valueOf(random.nextDouble() * 100));
                        order.setStatus("concurrent");
                        order.setCreatedAt(LocalDateTime.now());
                        
                        orderRepository.insert(order);
                        
                        if (!allIds.add(order.getId())) {
                            throw new AssertionError("Duplicate ID in concurrent insert: " + order.getId());
                        }
                        
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Thread " + threadId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        
        logger.info("✓ Concurrent inserts: " + successCount.get() + "/" + 
                   (threadCount * recordsPerThread) + " succeeded");
        
        if (successCount.get() != threadCount * recordsPerThread) {
            throw new AssertionError("Some concurrent inserts failed");
        }
    }
    
    private void testConcurrentReadsWrites() throws Exception {
        logger.info("TEST: Concurrent Read/Write Operations");
        
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean conflict = new AtomicBoolean(false);
        
        // Writer thread
        executorService.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setSenderId("writer");
                    sms.setReceiverId("reader");
                    sms.setMessage("Message " + i);
                    sms.setStatus("unread");
                    sms.setCreatedAt(LocalDateTime.now());
                    smsRepository.insert(sms);
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                conflict.set(true);
            } finally {
                latch.countDown();
            }
        });
        
        // Reader thread
        executorService.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    List<SmsEntity> messages = smsRepository.findAllByDateRange(
                        LocalDateTime.now().minusHours(1),
                        LocalDateTime.now()
                    );
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                conflict.set(true);
            } finally {
                latch.countDown();
            }
        });
        
        latch.await(30, TimeUnit.SECONDS);
        
        if (conflict.get()) {
            throw new AssertionError("Conflict detected in concurrent read/write");
        }
        
        logger.info("✓ Concurrent read/write operations completed without conflicts");
    }
    
    private void testConcurrentUpdates() throws Exception {
        logger.info("TEST: Concurrent Update Operations");
        
        // Insert initial record
        OrderEntity order = new OrderEntity();
        order.setCustomerId(5000L);
        order.setAmount(BigDecimal.valueOf(100.00));
        order.setStatus("pending");
        order.setCreatedAt(LocalDateTime.now());
        orderRepository.insert(order);
        
        Long orderId = order.getId();
        int threadCount = 5;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger updateCount = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            executorService.submit(() -> {
                try {
                    OrderEntity toUpdate = orderRepository.findById(orderId);
                    if (toUpdate != null) {
                        toUpdate.setStatus("updated_" + Thread.currentThread().getId());
                        toUpdate.setAmount(toUpdate.getAmount().add(BigDecimal.ONE));
                        orderRepository.updateById(orderId, toUpdate);
                        updateCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected - some updates may fail due to concurrent modification
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        
        logger.info("✓ Concurrent updates: " + updateCount.get() + "/" + threadCount + " succeeded");
    }
    
    private void testDeadlockHandling() throws Exception {
        logger.info("TEST: Deadlock Detection and Handling");
        
        // This test would need to create conditions for potential deadlock
        // For now, we'll test that operations don't hang indefinitely
        
        CompletableFuture<Boolean> future1 = CompletableFuture.supplyAsync(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    OrderEntity order = new OrderEntity();
                    order.setCustomerId(6000L + i);
                    order.setAmount(BigDecimal.valueOf(50.00));
                    order.setStatus("deadlock_test");
                    order.setCreatedAt(LocalDateTime.now());
                    orderRepository.insert(order);
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        
        CompletableFuture<Boolean> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    List<OrderEntity> orders = orderRepository.findAllByDateRange(
                        LocalDateTime.now().minusHours(1),
                        LocalDateTime.now()
                    );
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        
        try {
            CompletableFuture.allOf(future1, future2).get(10, TimeUnit.SECONDS);
            logger.info("✓ No deadlocks detected");
        } catch (TimeoutException e) {
            throw new AssertionError("Potential deadlock detected - operations timed out");
        }
    }
    
    private void testRaceConditions() throws Exception {
        logger.info("TEST: Race Condition Handling");
        
        // Test table/partition creation race condition
        LocalDateTime futureDate = LocalDateTime.now().plusDays(100);
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    startLatch.await(); // All threads start simultaneously
                    
                    OrderEntity order = new OrderEntity();
                    order.setCustomerId(7000L + threadId);
                    order.setAmount(BigDecimal.valueOf(25.00));
                    order.setStatus("race_test");
                    order.setCreatedAt(futureDate); // All trying to create same partition
                    
                    orderRepository.insert(order);
                    successCount.incrementAndGet();
                    
                } catch (Exception e) {
                    // Some may fail due to race condition
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        startLatch.countDown(); // Start race
        endLatch.await(10, TimeUnit.SECONDS);
        
        logger.info("✓ Race condition test: " + successCount.get() + "/" + threadCount + " succeeded");
        
        if (successCount.get() == 0) {
            throw new AssertionError("All threads failed - race condition not handled properly");
        }
    }
}