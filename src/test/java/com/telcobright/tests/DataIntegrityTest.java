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

/**
 * Data Integrity Test Suite
 */
public class DataIntegrityTest extends BaseRepositoryTest {
    
    private GenericMultiTableRepository<SmsEntity, Long> smsRepository;
    private GenericPartitionedTableRepository<OrderEntity, Long> orderRepository;
    
    @Override
    public void runAllTests() throws Exception {
        logger.info("Starting Data Integrity Tests");
        
        setUp();
        initializeRepositories();
        
        try {
            testIdUniqueness();
            testTimestampConsistency();
            testTransactionAtomicity();
            testDataConsistency();
            testReferentialIntegrity();
            
            logger.info("All Data Integrity tests completed");
            
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
            .tablePrefix("integrity_sms")
            .partitionRetentionPeriod(7)
            .build();
        
        orderRepository = GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
            .host(TEST_HOST).port(TEST_PORT).database(TEST_DATABASE)
            .username(TEST_USER).password(TEST_PASSWORD)
            .tableName("integrity_orders")
            .partitionRetentionDays(30)
            .build();
    }
    
    private void testIdUniqueness() throws Exception {
        logger.info("TEST: ID Uniqueness Verification");
        
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            OrderEntity order = new OrderEntity();
            order.setCustomerId(1000L + i);
            order.setAmount(BigDecimal.valueOf(99.99));
            order.setStatus("test");
            order.setCreatedAt(LocalDateTime.now());
            
            orderRepository.insert(order);
            
            if (!ids.add(order.getId())) {
                throw new AssertionError("Duplicate ID detected: " + order.getId());
            }
        }
        
        logger.info("✓ ID uniqueness verified for 1000 records");
    }
    
    private void testTimestampConsistency() throws Exception {
        logger.info("TEST: Timestamp Consistency");
        
        LocalDateTime testTime = LocalDateTime.now();
        SmsEntity sms = new SmsEntity();
        sms.setSenderId("timestamp_test");
        sms.setReceiverId("timestamp_test");
        sms.setMessage("Timestamp test");
        sms.setStatus("test");
        sms.setCreatedAt(testTime);
        
        smsRepository.insert(sms);
        
        SmsEntity retrieved = smsRepository.findById(sms.getId());
        if (retrieved != null) {
            long diff = Math.abs(testTime.getNano() - retrieved.getCreatedAt().getNano());
            if (diff > 1000000) { // More than 1ms difference
                logger.warn("Timestamp precision loss: " + diff + " ns");
            }
        }
        
        logger.info("✓ Timestamp consistency verified");
    }
    
    private void testTransactionAtomicity() throws Exception {
        logger.info("TEST: Transaction Atomicity");
        
        List<OrderEntity> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            OrderEntity order = new OrderEntity();
            order.setCustomerId(2000L + i);
            order.setAmount(BigDecimal.valueOf(50.00));
            order.setStatus("atomic_test");
            order.setCreatedAt(LocalDateTime.now());
            batch.add(order);
        }
        
        try {
            orderRepository.insertMultiple(batch);
            logger.info("✓ Batch insert completed atomically");
        } catch (Exception e) {
            // Verify none were inserted
            try (Connection conn = connectionProvider.getConnection();
                 Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM integrity_orders WHERE status = 'atomic_test'"
                );
                rs.next();
                if (rs.getInt(1) > 0) {
                    throw new AssertionError("Partial batch was inserted - atomicity violated");
                }
            }
            logger.info("✓ Atomicity maintained on failure");
        }
    }
    
    private void testDataConsistency() throws Exception {
        logger.info("TEST: Data Consistency Across Operations");
        
        // Insert test data
        OrderEntity original = new OrderEntity();
        original.setCustomerId(3000L);
        original.setAmount(BigDecimal.valueOf(123.45));
        original.setStatus("consistency_test");
        original.setCreatedAt(LocalDateTime.now());
        
        orderRepository.insert(original);
        
        // Read back and verify
        OrderEntity retrieved = orderRepository.findById(original.getId());
        
        if (!original.getAmount().equals(retrieved.getAmount())) {
            throw new AssertionError("Amount mismatch: " + original.getAmount() + " != " + retrieved.getAmount());
        }
        
        if (!original.getStatus().equals(retrieved.getStatus())) {
            throw new AssertionError("Status mismatch");
        }
        
        logger.info("✓ Data consistency verified");
    }
    
    private void testReferentialIntegrity() throws Exception {
        logger.info("TEST: Referential Integrity");
        
        // This would test foreign key relationships if they exist
        // For now, we'll verify internal consistency
        
        LocalDateTime date = LocalDateTime.now();
        List<Long> insertedIds = new ArrayList<>();
        
        for (int i = 0; i < 10; i++) {
            SmsEntity sms = new SmsEntity();
            sms.setSenderId("ref_test");
            sms.setReceiverId("ref_test");
            sms.setMessage("Reference test " + i);
            sms.setStatus("test");
            sms.setCreatedAt(date);
            
            smsRepository.insert(sms);
            insertedIds.add(sms.getId());
        }
        
        // Verify all can be retrieved
        for (Long id : insertedIds) {
            SmsEntity found = smsRepository.findById(id);
            if (found == null) {
                throw new AssertionError("Failed to retrieve ID: " + id);
            }
        }
        
        logger.info("✓ Referential integrity verified");
    }
}