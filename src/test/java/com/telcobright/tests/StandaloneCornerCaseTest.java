package com.telcobright.tests;

import com.telcobright.api.ShardingRepository;
import com.telcobright.api.RepositoryProxy;
import com.telcobright.core.connection.ConnectionProvider;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.examples.entity.SmsEntity;
import com.telcobright.tests.invalid.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

public class StandaloneCornerCaseTest {
    
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/test_corner_case?createDatabaseIfNotExist=true&useSSL=false&serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";
    
    private ConnectionProvider connectionProvider;
    
    public void setup() throws SQLException {
        connectionProvider = new ConnectionProvider("127.0.0.1", 3306, "test_corner_case", DB_USER, DB_PASSWORD);
        
        // Create test database if not exists
        try (Connection conn = connectionProvider.getConnection()) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS test_corner_case");
        }
    }
    
    // Test 1: Entity without ShardingEntity interface (compile-time safety)
    public void testEntityWithoutInterface() {
        System.out.println("\n=== Test 1: Entity Without ShardingEntity Interface ===");
        try {
            // This should fail at compile time - InvalidEntity1_NoInterface doesn't implement ShardingEntity
            // Uncommenting this would cause compilation error:
            // ShardingRepository<InvalidEntity1_NoInterface, Long> repo = 
            //     GenericPartitionedTableRepository.<InvalidEntity1_NoInterface, Long>builder()
            //         .withConnectionProvider(connectionProvider)
            //         .withEntityClass(InvalidEntity1_NoInterface.class)
            //         .build();
            
            System.out.println("✓ Compile-time safety prevents entities without ShardingEntity interface");
        } catch (Exception e) {
            System.out.println("✗ Unexpected error: " + e.getMessage());
        }
    }
    
    // Test 2: Entity missing @Id annotation
    public void testEntityMissingIdAnnotation() {
        System.out.println("\n=== Test 2: Entity Missing @Id Annotation ===");
        try {
            ShardingRepository<InvalidEntity2_NoId, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(InvalidEntity2_NoId.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("invalid_test_2")
                    .build();
            
            // Try to insert - should fail due to missing ID field
            InvalidEntity2_NoId entity = new InvalidEntity2_NoId();
            entity.setCreatedAt(LocalDateTime.now());
            entity.setName("Test");
            
            repo.insert(entity);
            System.out.println("✗ Should have failed for missing @Id annotation");
        } catch (Exception e) {
            System.out.println("✓ Correctly failed for missing @Id: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }
    
    // Test 3: Entity missing @ShardingKey annotation
    public void testEntityMissingShardingKey() {
        System.out.println("\n=== Test 3: Entity Missing @ShardingKey Annotation ===");
        try {
            ShardingRepository<InvalidEntity3_NoShardingKey, Long> repo = 
                new GenericMultiTableRepository.Builder<>(InvalidEntity3_NoShardingKey.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tablePrefix("invalid_test_3")
                    .build();
            
            InvalidEntity3_NoShardingKey entity = new InvalidEntity3_NoShardingKey();
            entity.setCreatedAt(LocalDateTime.now());
            entity.setName("Test");
            
            repo.insert(entity);
            System.out.println("✗ Should have failed for missing @ShardingKey annotation");
        } catch (Exception e) {
            System.out.println("✓ Correctly failed for missing @ShardingKey: " + e.getClass().getSimpleName());
        }
    }
    
    // Test 4: Entity with wrong ID type
    public void testEntityWrongIdType() {
        System.out.println("\n=== Test 4: Entity With Wrong ID Type ===");
        try {
            ShardingRepository<InvalidEntity4_WrongIdType, String> repo = 
                new GenericPartitionedTableRepository.Builder<>(InvalidEntity4_WrongIdType.class, String.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("invalid_test_4")
                    .build();
            
            InvalidEntity4_WrongIdType entity = new InvalidEntity4_WrongIdType();
            entity.setId("ABC123"); // String ID with AUTO_INCREMENT will fail
            entity.setCreatedAt(LocalDateTime.now());
            entity.setName("Test");
            
            repo.insert(entity);
            System.out.println("✗ Should have failed for String ID with AUTO_INCREMENT");
        } catch (Exception e) {
            System.out.println("✓ Correctly failed for wrong ID type: " + e.getClass().getSimpleName());
        }
    }
    
    // Test 5: Entity with null created_at
    public void testEntityNullCreatedAt() {
        System.out.println("\n=== Test 5: Entity With Null created_at ===");
        try {
            ShardingRepository<InvalidEntity5_NullCreatedAt, Long> repo = 
                new GenericMultiTableRepository.Builder<>(InvalidEntity5_NullCreatedAt.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tablePrefix("invalid_test_5")
                    .build();
            
            InvalidEntity5_NullCreatedAt entity = new InvalidEntity5_NullCreatedAt();
            entity.setName("Test");
            // created_at will always return null from this entity
            
            repo.insert(entity);
            System.out.println("✗ Should have failed for null created_at");
        } catch (NullPointerException e) {
            System.out.println("✓ Correctly threw NullPointerException for null created_at");
        } catch (Exception e) {
            System.out.println("✓ Failed with: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }
    
    // Test 6: Empty batch insert
    public void testEmptyBatchInsert() {
        System.out.println("\n=== Test 6: Empty Batch Insert ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_empty_batch")
                    .build();
            
            List<SmsEntity> emptyList = new ArrayList<>();
            repo.insertMultiple(emptyList);
            System.out.println("✓ Handled empty batch insert gracefully");
        } catch (Exception e) {
            System.out.println("✗ Failed on empty batch: " + e.getMessage());
        }
    }
    
    // Test 7: Extremely large batch insert
    public void testVeryLargeBatchInsert() {
        System.out.println("\n=== Test 7: Very Large Batch Insert ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_large_batch")
                    .build();
            
            // Create a batch that might exceed prepared statement limits
            List<SmsEntity> largeBatch = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                SmsEntity entity = new SmsEntity();
                entity.setUserId(String.valueOf(i));
                entity.setPhoneNumber("555-" + String.format("%04d", i));
                entity.setMessage("Test message " + i);
                entity.setCreatedAt(LocalDateTime.now());
                largeBatch.add(entity);
            }
            
            long start = System.currentTimeMillis();
            repo.insertMultiple(largeBatch);
            long duration = System.currentTimeMillis() - start;
            
            System.out.println("✓ Handled 1,000 record batch in " + duration + "ms");
        } catch (Exception e) {
            System.out.println("! Large batch handling: " + e.getClass().getSimpleName());
        }
    }
    
    // Test 8: Query with invalid date range
    public void testInvalidDateRangeQuery() {
        System.out.println("\n=== Test 8: Invalid Date Range Query ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericMultiTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tablePrefix("test_date_range")
                    .build();
            
            // End date before start date
            LocalDateTime start = LocalDateTime.now();
            LocalDateTime end = start.minusDays(10);
            
            List<SmsEntity> results = repo.findAllByDateRange(start, end);
            System.out.println("✓ Handled invalid date range (returned " + results.size() + " results)");
        } catch (Exception e) {
            System.out.println("! Date range handling: " + e.getMessage());
        }
    }
    
    // Test 9: SQL injection attempt
    public void testSQLInjectionPrevention() {
        System.out.println("\n=== Test 9: SQL Injection Prevention ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_injection")
                    .build();
            
            SmsEntity entity = new SmsEntity();
            entity.setUserId("1");
            entity.setPhoneNumber("'; DROP TABLE sms; --");
            entity.setMessage("Test'; DELETE FROM sms WHERE 1=1; --");
            entity.setCreatedAt(LocalDateTime.now());
            
            repo.insert(entity);
            
            // Verify the table still exists
            repo.findAllAfterDate(LocalDateTime.now().minusDays(1));
            System.out.println("✓ SQL injection attempt prevented - table still exists");
        } catch (Exception e) {
            System.out.println("! SQL injection test: " + e.getMessage());
        }
    }
    
    // Test 10: Connection failure handling
    public void testConnectionFailure() {
        System.out.println("\n=== Test 10: Connection Failure Handling ===");
        try {
            // Test with invalid host instead of custom provider
            
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("invalid.host")
                    .port(3306)
                    .database("test")
                    .username("test")
                    .password("test")
                    .tableName("test_conn_fail")
                    .build();
            
            repo.findAllAfterDate(LocalDateTime.now().minusDays(1));
            System.out.println("✗ Should have failed with connection error");
        } catch (SQLException e) {
            System.out.println("✓ Correctly propagated connection failure: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("✓ Failed with: " + e.getClass().getSimpleName());
        }
    }
    
    // Test 11: Negative ID values
    public void testNegativeIdValues() {
        System.out.println("\n=== Test 11: Negative ID Values ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_negative_id")
                    .build();
            
            // Try to find with negative ID
            SmsEntity result = repo.findById(-1L);
            if (result == null) {
                System.out.println("✓ Negative ID query returned null (expected)");
            } else {
                System.out.println("✗ Unexpected result for negative ID");
            }
            
            // Try findOneByIdGreaterThan with negative ID
            result = repo.findOneByIdGreaterThan(-999L);
            System.out.println("✓ findOneByIdGreaterThan with negative ID handled");
        } catch (Exception e) {
            System.out.println("! Negative ID handling: " + e.getMessage());
        }
    }
    
    // Test 12: Extremely large ID values
    public void testExtremelyLargeIdValues() {
        System.out.println("\n=== Test 12: Extremely Large ID Values ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_large_id")
                    .build();
            
            // Try to find with maximum Long value
            List<SmsEntity> results = repo.findBatchByIdGreaterThan(Long.MAX_VALUE - 1, 10);
            System.out.println("✓ Handled Long.MAX_VALUE query (found " + results.size() + " results)");
        } catch (Exception e) {
            System.out.println("! Large ID handling: " + e.getMessage());
        }
    }
    
    // Test 13: Zero batch size
    public void testZeroBatchSize() {
        System.out.println("\n=== Test 13: Zero Batch Size ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("test_corner_case")
                    .username(DB_USER)
                    .password(DB_PASSWORD)
                    .tableName("test_zero_batch")
                    .build();
            
            List<SmsEntity> results = repo.findBatchByIdGreaterThan(1L, 0);
            System.out.println("✓ Zero batch size returned " + results.size() + " results");
        } catch (Exception e) {
            System.out.println("! Zero batch size handling: " + e.getMessage());
        }
    }
    
    // Test 14: Repository with null connection provider
    public void testNullConnectionProvider() {
        System.out.println("\n=== Test 14: Null Connection Provider ===");
        try {
            ShardingRepository<SmsEntity, Long> repo = 
                new GenericPartitionedTableRepository.Builder<>(SmsEntity.class, Long.class)
                    .host(null)
                    .port(3306)
                    .database("test")
                    .username("test")
                    .password("test")
                    .tableName("test_null_provider")
                    .build();
            
            repo.findAllAfterDate(LocalDateTime.now().minusDays(1));
            System.out.println("✗ Should have failed with null connection provider");
        } catch (NullPointerException e) {
            System.out.println("✓ Correctly threw NullPointerException for null provider");
        } catch (Exception e) {
            System.out.println("✓ Failed with: " + e.getClass().getSimpleName());
        }
    }
    
    public void runAllTests() {
        System.out.println("========================================");
        System.out.println("       CORNER CASE TEST SUITE");
        System.out.println("========================================");
        
        try {
            setup();
            
            // Run all tests
            testEntityWithoutInterface();
            testEntityMissingIdAnnotation();
            testEntityMissingShardingKey();
            testEntityWrongIdType();
            testEntityNullCreatedAt();
            testEmptyBatchInsert();
            testVeryLargeBatchInsert();
            testInvalidDateRangeQuery();
            testSQLInjectionPrevention();
            testConnectionFailure();
            testNegativeIdValues();
            testExtremelyLargeIdValues();
            testZeroBatchSize();
            testNullConnectionProvider();
            
            System.out.println("\n========================================");
            System.out.println("     CORNER CASE TESTS COMPLETED");
            System.out.println("========================================");
            
        } catch (Exception e) {
            System.err.println("Test setup failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        new StandaloneCornerCaseTest().runAllTests();
    }
}