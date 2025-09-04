import com.telcobright.api.RepositoryProxy;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.examples.entity.OrderEntity;
import com.telcobright.examples.entity.SmsEntity;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class RunTests {
    
    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 3306;
    private static final String TEST_USER = "root";
    private static final String TEST_PASSWORD = "123456";
    private static final String TEST_DATABASE = "test";
    
    public static void main(String[] args) {
        System.out.println("==========================================================");
        System.out.println("    SHARDING REPOSITORY TEST EXECUTION");
        System.out.println("==========================================================");
        System.out.println("Start Time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        System.out.println();
        
        int totalTests = 0;
        int passedTests = 0;
        int failedTests = 0;
        
        // Test results collection
        Map<String, TestResult> results = new LinkedHashMap<>();
        
        // Test 1: Multi-Table Repository Basic Operations
        System.out.println("----------------------------------------------------------");
        System.out.println("TEST SUITE 1: MULTI-TABLE REPOSITORY");
        System.out.println("----------------------------------------------------------");
        
        TestResult multiTableResult = testMultiTableRepository();
        results.put("Multi-Table Repository", multiTableResult);
        totalTests++;
        if (multiTableResult.passed) passedTests++; else failedTests++;
        
        // Test 2: Partitioned Table Repository Basic Operations
        System.out.println("\n----------------------------------------------------------");
        System.out.println("TEST SUITE 2: PARTITIONED TABLE REPOSITORY");
        System.out.println("----------------------------------------------------------");
        
        TestResult partitionedResult = testPartitionedTableRepository();
        results.put("Partitioned Table Repository", partitionedResult);
        totalTests++;
        if (partitionedResult.passed) passedTests++; else failedTests++;
        
        // Test 3: Performance Benchmarks
        System.out.println("\n----------------------------------------------------------");
        System.out.println("TEST SUITE 3: PERFORMANCE BENCHMARKS");
        System.out.println("----------------------------------------------------------");
        
        TestResult perfResult = runPerformanceBenchmarks();
        results.put("Performance Benchmarks", perfResult);
        totalTests++;
        if (perfResult.passed) passedTests++; else failedTests++;
        
        // Test 4: Data Integrity
        System.out.println("\n----------------------------------------------------------");
        System.out.println("TEST SUITE 4: DATA INTEGRITY");
        System.out.println("----------------------------------------------------------");
        
        TestResult integrityResult = testDataIntegrity();
        results.put("Data Integrity", integrityResult);
        totalTests++;
        if (integrityResult.passed) passedTests++; else failedTests++;
        
        // Print Summary
        System.out.println("\n==========================================================");
        System.out.println("    TEST EXECUTION SUMMARY");
        System.out.println("==========================================================");
        System.out.println();
        
        System.out.println("Test Results:");
        System.out.println("-------------");
        for (Map.Entry<String, TestResult> entry : results.entrySet()) {
            String status = entry.getValue().passed ? "✅ PASSED" : "❌ FAILED";
            System.out.printf("%-30s: %s\n", entry.getKey(), status);
            if (entry.getValue().metrics != null && !entry.getValue().metrics.isEmpty()) {
                for (Map.Entry<String, String> metric : entry.getValue().metrics.entrySet()) {
                    System.out.printf("  %-28s: %s\n", metric.getKey(), metric.getValue());
                }
            }
            if (!entry.getValue().passed && entry.getValue().error != null) {
                System.out.println("  Error: " + entry.getValue().error);
            }
        }
        
        System.out.println();
        System.out.println("Overall Statistics:");
        System.out.println("------------------");
        System.out.println("Total Tests Run: " + totalTests);
        System.out.println("Tests Passed: " + passedTests);
        System.out.println("Tests Failed: " + failedTests);
        System.out.println("Success Rate: " + String.format("%.1f%%", (passedTests * 100.0 / totalTests)));
        
        System.out.println();
        System.out.println("End Time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        System.out.println("==========================================================");
        
        if (failedTests == 0) {
            System.out.println("🎉 ALL TESTS PASSED SUCCESSFULLY! 🎉");
            System.exit(0);
        } else {
            System.out.println("⚠️  SOME TESTS FAILED - SEE DETAILS ABOVE");
            System.exit(1);
        }
    }
    
    private static TestResult testMultiTableRepository() {
        TestResult result = new TestResult();
        
        try {
            GenericMultiTableRepository<SmsEntity, Long> smsRepository = 
                GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                    .host(TEST_HOST)
                    .port(TEST_PORT)
                    .database(TEST_DATABASE)
                    .username(TEST_USER)
                    .password(TEST_PASSWORD)
                    .tablePrefix("test_sms")
                    .partitionRetentionPeriod(7)
                    .build();
            
            long startTime = System.currentTimeMillis();
            int recordsInserted = 0;
            
            // Test 1: Insert records for different days
            System.out.println("Testing table auto-creation and data distribution...");
            for (int day = 0; day < 3; day++) {
                for (int i = 0; i < 10; i++) {
                    SmsEntity sms = new SmsEntity();
                    sms.setUserId("user_" + i);
                    sms.setPhoneNumber("555-000" + i);
                    sms.setMessage("Test message for day " + day);
                    sms.setStatus("SENT");
                    sms.setCreatedAt(LocalDateTime.now().minusDays(day));
                    
                    smsRepository.insert(sms);
                    recordsInserted++;
                }
            }
            
            // Test 2: Query across tables
            System.out.println("Testing cross-table queries...");
            List<SmsEntity> results = smsRepository.findAllByDateRange(
                LocalDateTime.now().minusDays(3),
                LocalDateTime.now()
            );
            
            // Test 3: Cursor pagination
            System.out.println("Testing cursor-based pagination...");
            Long cursor = 0L;
            int batchCount = 0;
            while (batchCount < 3) {
                List<SmsEntity> batch = smsRepository.findBatchByIdGreaterThan(cursor, 5);
                if (batch.isEmpty()) break;
                cursor = batch.get(batch.size() - 1).getId();
                batchCount++;
            }
            
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify results
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:mysql://" + TEST_HOST + ":" + TEST_PORT + "/" + TEST_DATABASE,
                    TEST_USER, TEST_PASSWORD);
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as table_count FROM information_schema.tables " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' AND table_name LIKE 'test_sms_%'"
                );
                
                rs.next();
                int tableCount = rs.getInt("table_count");
                
                result.metrics.put("Records Inserted", String.valueOf(recordsInserted));
                result.metrics.put("Tables Created", String.valueOf(tableCount));
                result.metrics.put("Query Results", String.valueOf(results.size()));
                result.metrics.put("Duration", duration + " ms");
                result.metrics.put("Insert TPS", String.format("%.0f", recordsInserted * 1000.0 / duration));
                
                result.passed = recordsInserted == 30 && tableCount >= 3 && results.size() > 0;
                
                System.out.println("✓ Multi-table operations completed successfully");
            }
            
            smsRepository.shutdown();
            
        } catch (Exception e) {
            result.passed = false;
            result.error = e.getMessage();
            System.err.println("✗ Multi-table test failed: " + e.getMessage());
        }
        
        return result;
    }
    
    private static TestResult testPartitionedTableRepository() {
        TestResult result = new TestResult();
        
        try {
            GenericPartitionedTableRepository<OrderEntity, Long> orderRepository = 
                GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                    .host(TEST_HOST)
                    .port(TEST_PORT)
                    .database(TEST_DATABASE)
                    .username(TEST_USER)
                    .password(TEST_PASSWORD)
                    .tableName("test_orders")
                    .partitionRetentionPeriod(30)
                    .build();
            
            long startTime = System.currentTimeMillis();
            int recordsInserted = 0;
            
            // Test 1: Insert records for different days
            System.out.println("Testing partition creation...");
            for (int day = 0; day < 5; day++) {
                for (int i = 0; i < 10; i++) {
                    OrderEntity order = new OrderEntity();
                    order.setCustomerId("CUST" + (1000 + day * 10 + i));
                    order.setOrderNumber("ORD" + (1000 + day * 10 + i));
                    order.setTotalAmount(BigDecimal.valueOf(99.99 + i));
                    order.setStatus("PENDING");
                    order.setCreatedAt(LocalDateTime.now().minusDays(day));
                    
                    orderRepository.insert(order);
                    recordsInserted++;
                }
            }
            
            // Test 2: Query with partition pruning
            System.out.println("Testing partition pruning...");
            LocalDateTime queryDate = LocalDateTime.now().minusDays(2);
            List<OrderEntity> dayOrders = orderRepository.findAllByDateRange(
                queryDate.withHour(0).withMinute(0),
                queryDate.withHour(23).withMinute(59)
            );
            
            // Test 3: Cursor operations
            System.out.println("Testing cursor operations...");
            OrderEntity next = orderRepository.findOneByIdGreaterThan(0L);
            
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify partitions
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:mysql://" + TEST_HOST + ":" + TEST_PORT + "/" + TEST_DATABASE,
                    TEST_USER, TEST_PASSWORD);
                 Statement stmt = conn.createStatement()) {
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as partition_count FROM information_schema.partitions " +
                    "WHERE table_schema = '" + TEST_DATABASE + "' AND table_name = 'test_orders' " +
                    "AND partition_name IS NOT NULL"
                );
                
                rs.next();
                int partitionCount = rs.getInt("partition_count");
                
                result.metrics.put("Records Inserted", String.valueOf(recordsInserted));
                result.metrics.put("Partitions Created", String.valueOf(partitionCount));
                result.metrics.put("Day Query Results", String.valueOf(dayOrders.size()));
                result.metrics.put("Duration", duration + " ms");
                result.metrics.put("Insert TPS", String.format("%.0f", recordsInserted * 1000.0 / duration));
                
                result.passed = recordsInserted == 50 && partitionCount >= 5;
                
                System.out.println("✓ Partitioned table operations completed successfully");
            }
            
            orderRepository.shutdown();
            
        } catch (Exception e) {
            result.passed = false;
            result.error = e.getMessage();
            System.err.println("✗ Partitioned table test failed: " + e.getMessage());
        }
        
        return result;
    }
    
    private static TestResult runPerformanceBenchmarks() {
        TestResult result = new TestResult();
        
        try {
            System.out.println("Running performance benchmarks...");
            
            GenericPartitionedTableRepository<OrderEntity, Long> repository = 
                GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                    .host(TEST_HOST)
                    .port(TEST_PORT)
                    .database(TEST_DATABASE)
                    .username(TEST_USER)
                    .password(TEST_PASSWORD)
                    .tableName("perf_orders")
                    .partitionRetentionPeriod(30)
                    .build();
            
            // Batch insert test
            System.out.println("Testing batch insert performance...");
            List<OrderEntity> batch = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                OrderEntity order = new OrderEntity();
                order.setCustomerId("CUST" + (2000 + i));
                order.setOrderNumber("ORD" + (2000 + i));
                order.setTotalAmount(BigDecimal.valueOf(Math.random() * 1000));
                order.setStatus("CONFIRMED");
                order.setCreatedAt(LocalDateTime.now());
                batch.add(order);
            }
            
            long insertStart = System.currentTimeMillis();
            repository.insertMultiple(batch);
            long insertDuration = System.currentTimeMillis() - insertStart;
            
            double insertTPS = 1000.0 * 1000 / insertDuration;
            
            // Query performance test
            System.out.println("Testing query performance...");
            long queryTotal = 0;
            int iterations = 100;
            
            for (int i = 0; i < iterations; i++) {
                long queryStart = System.nanoTime();
                repository.findById(2000L + (long)(Math.random() * 1000));
                queryTotal += (System.nanoTime() - queryStart);
            }
            
            double avgQueryMs = queryTotal / 1_000_000.0 / iterations;
            
            result.metrics.put("Batch Insert TPS", String.format("%.0f", insertTPS));
            result.metrics.put("Avg Query Latency", String.format("%.2f ms", avgQueryMs));
            result.metrics.put("Batch Size", "1000");
            result.metrics.put("Query Iterations", String.valueOf(iterations));
            
            // Performance thresholds
            result.passed = insertTPS > 500 && avgQueryMs < 50;
            
            if (result.passed) {
                System.out.println("✓ Performance benchmarks passed thresholds");
            } else {
                System.out.println("✗ Performance below expected thresholds");
            }
            
            repository.shutdown();
            
        } catch (Exception e) {
            result.passed = false;
            result.error = e.getMessage();
            System.err.println("✗ Performance benchmark failed: " + e.getMessage());
        }
        
        return result;
    }
    
    private static TestResult testDataIntegrity() {
        TestResult result = new TestResult();
        
        try {
            System.out.println("Testing data integrity...");
            
            GenericMultiTableRepository<SmsEntity, Long> repository = 
                GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                    .host(TEST_HOST)
                    .port(TEST_PORT)
                    .database(TEST_DATABASE)
                    .username(TEST_USER)
                    .password(TEST_PASSWORD)
                    .tablePrefix("integrity_test")
                    .partitionRetentionPeriod(7)
                    .build();
            
            // Test ID uniqueness
            System.out.println("Testing ID uniqueness...");
            Set<Long> ids = new HashSet<>();
            boolean duplicateFound = false;
            
            for (int i = 0; i < 100; i++) {
                SmsEntity sms = new SmsEntity();
                sms.setUserId("integrity_test");
                sms.setPhoneNumber("555-0000");
                sms.setMessage("Integrity test " + i);
                sms.setStatus("test");
                sms.setCreatedAt(LocalDateTime.now());
                
                repository.insert(sms);
                
                if (!ids.add(sms.getId())) {
                    duplicateFound = true;
                    break;
                }
            }
            
            // Test data consistency
            System.out.println("Testing data consistency...");
            SmsEntity testEntity = new SmsEntity();
            testEntity.setUserId("consistency_test");
            testEntity.setPhoneNumber("555-1234");
            testEntity.setMessage("Test message with special chars: 你好 😀");
            testEntity.setStatus("pending");
            testEntity.setCreatedAt(LocalDateTime.now());
            
            repository.insert(testEntity);
            
            SmsEntity retrieved = repository.findById(testEntity.getId());
            boolean dataConsistent = retrieved != null && 
                testEntity.getMessage().equals(retrieved.getMessage()) &&
                testEntity.getStatus().equals(retrieved.getStatus());
            
            result.metrics.put("IDs Tested", "100");
            result.metrics.put("Duplicate IDs", duplicateFound ? "Found" : "None");
            result.metrics.put("Data Consistency", dataConsistent ? "Verified" : "Failed");
            
            result.passed = !duplicateFound && dataConsistent;
            
            if (result.passed) {
                System.out.println("✓ Data integrity verified");
            } else {
                System.out.println("✗ Data integrity issues detected");
            }
            
            repository.shutdown();
            
        } catch (Exception e) {
            result.passed = false;
            result.error = e.getMessage();
            System.err.println("✗ Data integrity test failed: " + e.getMessage());
        }
        
        return result;
    }
    
    static class TestResult {
        boolean passed = false;
        String error = null;
        Map<String, String> metrics = new LinkedHashMap<>();
    }
}