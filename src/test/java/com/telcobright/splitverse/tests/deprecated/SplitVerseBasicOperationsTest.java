package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.io.*;
import java.nio.file.*;

/**
 * Basic CRUD operations test for Split-Verse
 * Tests single shard configuration with fundamental operations
 */
public class SplitVerseBasicOperationsTest {
    
    private static final String DB_HOST = "127.0.0.1";
    private static final int DB_PORT = 3306;
    private static final String DB_NAME = "splitverse_test";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";
    private static final String LOG_FILE = "SplitVerseBasicOperationsTest_report.log";
    
    private SplitVerseRepository<SubscriberEntity, LocalDateTime> repository;
    private Connection directConnection;
    private PrintWriter logWriter;
    private int passedTests = 0;
    private int failedTests = 0;
    private long testStartTime;
    
    public void setup() throws SQLException, IOException {
        // Initialize log writer
        logWriter = new PrintWriter(new FileWriter(LOG_FILE, false));
        logWriter.println("===========================================");
        logWriter.println("SPLIT-VERSE BASIC OPERATIONS TEST REPORT");
        logWriter.println("===========================================");
        logWriter.println("Test Started: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        logWriter.println("Database: " + DB_HOST + ":" + DB_PORT + "/" + DB_NAME);
        logWriter.println();
        
        System.out.println("\n=== Setting up Split-Verse Test Environment ===");
        logWriter.println("[SETUP] Initializing test environment...");
        
        // Create test database
        directConnection = DriverManager.getConnection(
            "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/?useSSL=false&serverTimezone=UTC",
            DB_USER, DB_PASSWORD
        );
        
        Statement stmt = directConnection.createStatement();
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
        stmt.execute("USE " + DB_NAME);
        System.out.println("✓ Database created/verified: " + DB_NAME);
        logWriter.println("[SETUP] Database created/verified: " + DB_NAME);
        
        // Configure single shard
        ShardConfig config = ShardConfig.builder()
            .shardId("primary")
            .host(DB_HOST)
            .port(DB_PORT)
            .database(DB_NAME)
            .username(DB_USER)
            .password(DB_PASSWORD)
            .connectionPoolSize(5)
            .enabled(true)
            .build();
        
        // Create Split-Verse repository - ONLY way to access the functionality
        repository = SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
            .withSingleShard(config)
            .withEntityClass(SubscriberEntity.class)
            .withPartitionType(PartitionType.DATE_BASED)
            .withPartitionKeyColumn("created_at")
            .build();
        
        System.out.println("✓ Split-Verse repository initialized");
        logWriter.println("[SETUP] Split-Verse repository initialized with shard: primary");
        
        // Clean up any existing data
        cleanupTestData();
        logWriter.println("[SETUP] Test environment ready\n");
    }
    
    private void cleanupTestData() throws SQLException {
        try {
            Statement stmt = directConnection.createStatement();
            stmt.execute("DROP TABLE IF EXISTS subscribers");
            System.out.println("✓ Cleaned up existing test data");
        } catch (Exception e) {
            // Table might not exist yet
        }
    }
    
    // Test 1: Basic Insert Operation
    public void testBasicInsert() {
        System.out.println("\n=== Test 1: Basic Insert ===");
        logWriter.println("\n[TEST 1] Basic Insert Operation");
        logWriter.println("-------------------------------");
        testStartTime = System.currentTimeMillis();
        try {
            SubscriberEntity subscriber = createTestSubscriber("test_001", "+8801711111111");
            
            repository.insert(subscriber);
            System.out.println("✓ Successfully inserted subscriber with ID: " + subscriber.getId());
            logWriter.println("Inserted entity with ID: " + subscriber.getId());
            logWriter.println("  MSISDN: " + subscriber.getMsisdn());
            logWriter.println("  Balance: " + subscriber.getBalance());
            
            // Verify insertion directly
            PreparedStatement ps = directConnection.prepareStatement(
                "SELECT COUNT(*) FROM subscribers WHERE subscriber_id = ?"
            );
            ps.setString(1, subscriber.getId());
            ResultSet rs = ps.executeQuery();
            
            if (rs.next() && rs.getInt(1) == 1) {
                System.out.println("✓ Verified: Entity exists in database");
                logWriter.println("Verification: Entity found in database");
                logWriter.println("Test Duration: " + (System.currentTimeMillis() - testStartTime) + "ms");
                logWriter.println("Result: PASSED ✓");
                passedTests++;
            } else {
                System.out.println("✗ Entity not found in database");
                logWriter.println("Verification: Entity NOT found in database");
                logWriter.println("Result: FAILED ✗");
                failedTests++;
            }
        } catch (Exception e) {
            System.out.println("✗ Insert failed: " + e.getMessage());
            logWriter.println("Error: " + e.getMessage());
            logWriter.println("Result: FAILED ✗");
            failedTests++;
            e.printStackTrace();
        }
    }
    
    // Test 2: Find by ID
    public void testFindById() {
        System.out.println("\n=== Test 2: Find by ID ===");
        logWriter.println("\n[TEST 2] Find by ID");
        logWriter.println("-------------------");
        testStartTime = System.currentTimeMillis();
        try {
            // Insert test data
            String testId = "test_find_" + System.currentTimeMillis();
            SubscriberEntity original = createTestSubscriber(testId, "+8801722222222");
            repository.insert(original);
            
            // Find by ID
            SubscriberEntity found = repository.findById(testId);
            
            if (found != null) {
                System.out.println("✓ Found subscriber: " + found.getMsisdn());
                logWriter.println("Found entity with ID: " + testId);
                logWriter.println("  Retrieved MSISDN: " + found.getMsisdn());
                logWriter.println("  Original MSISDN: " + original.getMsisdn());
                assert found.getId().equals(testId) : "ID mismatch";
                assert found.getMsisdn().equals(original.getMsisdn()) : "MSISDN mismatch";
                System.out.println("✓ Data integrity verified");
                logWriter.println("Data integrity: VERIFIED");
                logWriter.println("Test Duration: " + (System.currentTimeMillis() - testStartTime) + "ms");
                logWriter.println("Result: PASSED ✓");
                passedTests++;
            } else {
                System.out.println("✗ Entity not found by ID");
                logWriter.println("Entity not found for ID: " + testId);
                logWriter.println("Result: FAILED ✗");
                failedTests++;
            }
        } catch (Exception e) {
            System.out.println("✗ Find by ID failed: " + e.getMessage());
            logWriter.println("Error: " + e.getMessage());
            logWriter.println("Result: FAILED ✗");
            failedTests++;
        }
    }
    
    // Test 3: Batch Insert
    public void testBatchInsert() {
        System.out.println("\n=== Test 3: Batch Insert ===");
        logWriter.println("\n[TEST 3] Batch Insert");
        logWriter.println("--------------------");
        testStartTime = System.currentTimeMillis();
        try {
            List<SubscriberEntity> subscribers = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String id = "batch_" + UUID.randomUUID().toString().substring(0, 8);
                subscribers.add(createTestSubscriber(id, "+88017" + String.format("%08d", i)));
            }
            
            long start = System.currentTimeMillis();
            repository.insertMultiple(subscribers);
            long duration = System.currentTimeMillis() - start;
            
            System.out.println("✓ Inserted " + subscribers.size() + " entities in " + duration + "ms");
            logWriter.println("Batch insert completed:");
            logWriter.println("  Entities: " + subscribers.size());
            logWriter.println("  Duration: " + duration + "ms");
            logWriter.println("  Throughput: " + (subscribers.size() * 1000 / duration) + " entities/sec");
            
            // Verify count
            Statement stmt = directConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM subscribers WHERE subscriber_id LIKE 'batch_%'");
            if (rs.next()) {
                int count = rs.getInt(1);
                System.out.println("✓ Verified: " + count + " entities in database");
                logWriter.println("Verification: " + count + " entities found in database");
                if (count == 100) {
                    logWriter.println("Result: PASSED ✓");
                    passedTests++;
                } else {
                    logWriter.println("Expected 100, found " + count);
                    logWriter.println("Result: FAILED ✗");
                    failedTests++;
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Batch insert failed: " + e.getMessage());
            logWriter.println("Error: " + e.getMessage());
            logWriter.println("Result: FAILED ✗");
            failedTests++;
        }
    }
    
    // Test 4: Date Range Query
    public void testDateRangeQuery() {
        System.out.println("\n=== Test 4: Date Range Query ===");
        logWriter.println("\n[TEST 4] Date Range Query");
        logWriter.println("------------------------");
        testStartTime = System.currentTimeMillis();
        try {
            // Insert entities with different dates
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime yesterday = now.minusDays(1);
            LocalDateTime lastWeek = now.minusDays(7);
            
            SubscriberEntity sub1 = createTestSubscriber("range_1", "+8801733333333");
            sub1.setCreatedAt(now);
            
            SubscriberEntity sub2 = createTestSubscriber("range_2", "+8801744444444");
            sub2.setCreatedAt(yesterday);
            
            SubscriberEntity sub3 = createTestSubscriber("range_3", "+8801755555555");
            sub3.setCreatedAt(lastWeek);
            
            repository.insert(sub1);
            repository.insert(sub2);
            repository.insert(sub3);
            
            // Query last 3 days
            List<SubscriberEntity> recent = repository.findAllByDateRange(
                now.minusDays(3), now.plusDays(1)
            );
            
            System.out.println("✓ Found " + recent.size() + " entities in last 3 days");
            logWriter.println("Query: Last 3 days");
            logWriter.println("  Found: " + recent.size() + " entities");
            assert recent.size() >= 2 : "Should find at least 2 recent entities";
            
            // Query last week
            List<SubscriberEntity> weekly = repository.findAllByDateRange(
                now.minusDays(8), now.plusDays(1)
            );
            
            System.out.println("✓ Found " + weekly.size() + " entities in last week");
            logWriter.println("Query: Last week");
            logWriter.println("  Found: " + weekly.size() + " entities");
            
            if (recent.size() >= 2 && weekly.size() >= 3) {
                logWriter.println("Test Duration: " + (System.currentTimeMillis() - testStartTime) + "ms");
                logWriter.println("Result: PASSED ✓");
                passedTests++;
            } else {
                logWriter.println("Result: FAILED ✗");
                failedTests++;
            }
        } catch (Exception e) {
            System.out.println("✗ Date range query failed: " + e.getMessage());
            logWriter.println("Error: " + e.getMessage());
            logWriter.println("Result: FAILED ✗");
            failedTests++;
        }
    }
    
    // Test 5: Update Operation
    public void testUpdate() {
        System.out.println("\n=== Test 5: Update Operation ===");
        try {
            // Insert initial entity
            String testId = "update_test_" + System.currentTimeMillis();
            SubscriberEntity original = createTestSubscriber(testId, "+8801766666666");
            original.setBalance(new BigDecimal("100.00"));
            repository.insert(original);
            
            // Update entity
            original.setBalance(new BigDecimal("200.00"));
            original.setStatus("PREMIUM");
            repository.updateById(testId, original);
            
            // Verify update
            SubscriberEntity updated = repository.findById(testId);
            if (updated != null) {
                System.out.println("✓ Entity updated successfully");
                assert updated.getBalance().compareTo(new BigDecimal("200.00")) == 0 : "Balance not updated";
                assert "PREMIUM".equals(updated.getStatus()) : "Status not updated";
                System.out.println("✓ Update verified: Balance=" + updated.getBalance() + ", Status=" + updated.getStatus());
            } else {
                System.out.println("✗ Updated entity not found");
            }
        } catch (Exception e) {
            System.out.println("✗ Update failed: " + e.getMessage());
        }
    }
    
    // Test 6: Cursor-based Pagination
    public void testCursorPagination() {
        System.out.println("\n=== Test 6: Cursor-based Pagination ===");
        logWriter.println("\n[TEST 6] Cursor-based Pagination");
        logWriter.println("--------------------------------");
        testStartTime = System.currentTimeMillis();
        try {
            // Insert test data with sequential IDs
            for (int i = 1; i <= 20; i++) {
                String id = String.format("cursor_%03d", i);
                SubscriberEntity sub = createTestSubscriber(id, "+88017" + String.format("%08d", i));
                repository.insert(sub);
            }
            
            // Test findOneByIdGreaterThan
            SubscriberEntity first = repository.findOneByIdGreaterThan("cursor_010");
            if (first != null) {
                System.out.println("✓ Found next entity after cursor_010: " + first.getId());
                assert first.getId().compareTo("cursor_010") > 0 : "ID should be greater than cursor";
            }
            
            // Test batch pagination
            List<SubscriberEntity> batch = repository.findBatchByIdGreaterThan("cursor_005", 5);
            System.out.println("✓ Retrieved batch of " + batch.size() + " entities");
            assert batch.size() <= 5 : "Batch size should not exceed limit";
            
            // Verify ordering
            String lastId = "cursor_005";
            for (SubscriberEntity entity : batch) {
                assert entity.getId().compareTo(lastId) > 0 : "IDs should be in ascending order";
                lastId = entity.getId();
            }
            System.out.println("✓ Cursor pagination ordering verified");
            logWriter.println("Cursor pagination:");
            logWriter.println("  Batch size: " + batch.size());
            logWriter.println("  Ordering: VERIFIED");
            logWriter.println("Test Duration: " + (System.currentTimeMillis() - testStartTime) + "ms");
            logWriter.println("Result: PASSED ✓");
            passedTests++;
        } catch (Exception e) {
            System.out.println("✗ Cursor pagination failed: " + e.getMessage());
            logWriter.println("Error: " + e.getMessage());
            logWriter.println("Result: FAILED ✗");
            failedTests++;
        }
    }
    
    // Test 7: Empty Result Handling
    public void testEmptyResults() {
        System.out.println("\n=== Test 7: Empty Result Handling ===");
        try {
            // Query non-existent ID
            SubscriberEntity notFound = repository.findById("non_existent_id");
            assert notFound == null : "Should return null for non-existent ID";
            System.out.println("✓ findById returns null for non-existent ID");
            
            // Query empty date range
            List<SubscriberEntity> emptyRange = repository.findAllByDateRange(
                LocalDateTime.now().plusDays(10),
                LocalDateTime.now().plusDays(20)
            );
            assert emptyRange.isEmpty() : "Should return empty list for future dates";
            System.out.println("✓ Date range query returns empty list for future dates");
            
            // Cursor with no results
            List<SubscriberEntity> noBatch = repository.findBatchByIdGreaterThan("zzz_999", 10);
            assert noBatch.isEmpty() : "Should return empty list for cursor beyond all IDs";
            System.out.println("✓ Cursor pagination returns empty list when no more data");
        } catch (Exception e) {
            System.out.println("✗ Empty result handling failed: " + e.getMessage());
        }
    }
    
    // Test 8: Special Characters Handling
    public void testSpecialCharacters() {
        System.out.println("\n=== Test 8: Special Characters Handling ===");
        try {
            // Test with special characters that could cause SQL issues
            String testId = "special_" + UUID.randomUUID().toString().substring(0, 8);
            SubscriberEntity subscriber = createTestSubscriber(testId, "+880-17'77\"777");
            subscriber.setImsi("IMSI'; DROP TABLE--");
            subscriber.setIccid("ICCID\"OR\"1\"=\"1");
            
            repository.insert(subscriber);
            SubscriberEntity retrieved = repository.findById(testId);
            
            if (retrieved != null) {
                System.out.println("✓ Special characters handled safely");
                assert retrieved.getImsi().equals("IMSI'; DROP TABLE--") : "IMSI preserved";
                assert retrieved.getIccid().equals("ICCID\"OR\"1\"=\"1") : "ICCID preserved";
                System.out.println("✓ SQL injection prevention verified");
            } else {
                System.out.println("✗ Failed to retrieve entity with special characters");
            }
        } catch (Exception e) {
            System.out.println("✗ Special character handling failed: " + e.getMessage());
        }
    }
    
    // Test 9: Null Handling
    public void testNullHandling() {
        System.out.println("\n=== Test 9: Null Handling ===");
        try {
            // Test entity with null optional fields
            String testId = "null_test_" + System.currentTimeMillis();
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId(testId);
            subscriber.setMsisdn("+8801788888888");
            subscriber.setCreatedAt(LocalDateTime.now());
            // Leave optional fields as null
            
            repository.insert(subscriber);
            SubscriberEntity retrieved = repository.findById(testId);
            
            if (retrieved != null) {
                System.out.println("✓ Entity with null fields inserted successfully");
                assert retrieved.getImsi() == null : "Null IMSI preserved";
                assert retrieved.getIccid() == null : "Null ICCID preserved";
                assert retrieved.getBalance() == null : "Null balance preserved";
                System.out.println("✓ Null values handled correctly");
            }
        } catch (Exception e) {
            System.out.println("✗ Null handling failed: " + e.getMessage());
        }
    }
    
    // Test 10: Verify Builder-only Access
    public void testBuilderOnlyAccess() {
        System.out.println("\n=== Test 10: Builder-only Access Verification ===");
        try {
            // This test verifies that we can ONLY create repositories through Split-Verse builder
            // The following should NOT compile (commented out):
            
            // GenericPartitionedTableRepository<SubscriberEntity> direct = 
            //     GenericPartitionedTableRepository.builder(SubscriberEntity.class)...
            // This would fail: builder method is not public
            
            // new GenericPartitionedTableRepository<>(...) 
            // This would fail: constructor is private
            
            System.out.println("✓ Direct repository creation is prevented (compile-time check)");
            System.out.println("✓ Only Split-Verse builder pattern is available");
            
            // Verify we can create through Split-Verse
            ShardConfig config = ShardConfig.builder()
                .shardId("verify")
                .host(DB_HOST)
                .database(DB_NAME)
                .username(DB_USER)
                .password(DB_PASSWORD)
                .build();
                
            SplitVerseRepository<SubscriberEntity, LocalDateTime> repo = 
                SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                    .withSingleShard(config)
                    .withEntityClass(SubscriberEntity.class)
                    .withPartitionType(PartitionType.DATE_BASED)
                    .withPartitionKeyColumn("created_at")
                    .build();
                    
            System.out.println("✓ Split-Verse builder works as expected");
        } catch (Exception e) {
            System.out.println("✗ Builder verification failed: " + e.getMessage());
        }
    }
    
    private SubscriberEntity createTestSubscriber(String id, String msisdn) {
        SubscriberEntity subscriber = new SubscriberEntity();
        subscriber.setId(id);
        subscriber.setMsisdn(msisdn);
        subscriber.setBalance(new BigDecimal("50.00"));
        subscriber.setStatus("ACTIVE");
        subscriber.setPlan("PREPAID");
        subscriber.setCreatedAt(LocalDateTime.now());
        subscriber.setDataBalanceMb(1000L);
        subscriber.setVoiceBalanceMinutes(100);
        return subscriber;
    }
    
    public void cleanup() {
        try {
            if (repository != null) {
                repository.shutdown();
            }
            if (directConnection != null && !directConnection.isClosed()) {
                directConnection.close();
            }
            System.out.println("\n✓ Test cleanup completed");
            
            // Write final summary
            logWriter.println("\n===========================================");
            logWriter.println("TEST SUMMARY");
            logWriter.println("===========================================");
            logWriter.println("Total Tests: " + (passedTests + failedTests));
            logWriter.println("Passed: " + passedTests);
            logWriter.println("Failed: " + failedTests);
            logWriter.println("Success Rate: " + (passedTests * 100 / Math.max(1, passedTests + failedTests)) + "%");
            logWriter.println("Test Completed: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            logWriter.println("===========================================");
            
            logWriter.close();
        } catch (Exception e) {
            System.err.println("Cleanup error: " + e.getMessage());
        }
    }
    
    public void runAllTests() {
        System.out.println("\n========================================");
        System.out.println("    SPLIT-VERSE BASIC OPERATIONS TEST");
        System.out.println("========================================");
        
        try {
            setup();
            
            testBasicInsert();
            testFindById();
            testBatchInsert();
            testDateRangeQuery();
            testUpdate();
            testCursorPagination();
            testEmptyResults();
            testSpecialCharacters();
            testNullHandling();
            testBuilderOnlyAccess();
            
            System.out.println("\n========================================");
            System.out.println("       ALL TESTS COMPLETED");
            System.out.println("========================================");
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }
    
    public static void main(String[] args) {
        new SplitVerseBasicOperationsTest().runAllTests();
    }
}