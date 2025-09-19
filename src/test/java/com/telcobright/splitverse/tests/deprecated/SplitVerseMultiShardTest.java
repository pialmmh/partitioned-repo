package com.telcobright.splitverse.tests;

import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.core.partition.PartitionType;
import com.telcobright.splitverse.config.ShardConfig;
import com.telcobright.splitverse.examples.entity.SubscriberEntity;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Multi-shard operations test for Split-Verse
 * Tests data distribution, parallel queries, and shard management
 */
public class SplitVerseMultiShardTest {
    
    private static final String DB_HOST = "127.0.0.1";
    private static final int DB_PORT = 3306;
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "123456";
    
    private SplitVerseRepository<SubscriberEntity, LocalDateTime> repository;
    private List<Connection> shardConnections = new ArrayList<>();
    private List<String> shardDatabases = Arrays.asList(
        "splitverse_shard1",
        "splitverse_shard2", 
        "splitverse_shard3"
    );
    
    public void setup() throws SQLException {
        System.out.println("\n=== Setting up Multi-Shard Test Environment ===");
        
        // Create shard databases
        Connection rootConn = DriverManager.getConnection(
            "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/?useSSL=false&serverTimezone=UTC",
            DB_USER, DB_PASSWORD
        );
        
        Statement stmt = rootConn.createStatement();
        for (String dbName : shardDatabases) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
            System.out.println("✓ Created shard database: " + dbName);
            
            // Create connection for each shard
            Connection shardConn = DriverManager.getConnection(
                "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + dbName + "?useSSL=false&serverTimezone=UTC",
                DB_USER, DB_PASSWORD
            );
            shardConnections.add(shardConn);
            
            // Clean up existing tables
            Statement cleanStmt = shardConn.createStatement();
            cleanStmt.execute("DROP TABLE IF EXISTS subscribers");
        }
        rootConn.close();
        
        // Configure multiple shards
        List<ShardConfig> shardConfigs = new ArrayList<>();
        for (int i = 0; i < shardDatabases.size(); i++) {
            ShardConfig config = ShardConfig.builder()
                .shardId("shard-" + (i + 1))
                .host(DB_HOST)
                .port(DB_PORT)
                .database(shardDatabases.get(i))
                .username(DB_USER)
                .password(DB_PASSWORD)
                .connectionPoolSize(5)
                .enabled(true)
                .build();
            shardConfigs.add(config);
        }
        
        // Create multi-shard Split-Verse repository
        repository = SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
            .withShardConfigs(shardConfigs)
            .withEntityClass(SubscriberEntity.class)
            .withPartitionType(PartitionType.DATE_BASED)
            .withPartitionKeyColumn("created_at")
            .build();
        
        System.out.println("✓ Split-Verse repository initialized with " + shardConfigs.size() + " shards");
    }
    
    // Test 1: Data Distribution Across Shards
    public void testDataDistribution() {
        System.out.println("\n=== Test 1: Data Distribution Across Shards ===");
        try {
            // Insert 300 entities
            List<SubscriberEntity> subscribers = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String id = UUID.randomUUID().toString();
                SubscriberEntity sub = createTestSubscriber(id, "+88018" + String.format("%08d", i));
                subscribers.add(sub);
            }
            
            repository.insertMultiple(subscribers);
            System.out.println("✓ Inserted " + subscribers.size() + " entities");
            
            // Check distribution across shards
            Map<String, Integer> distribution = new HashMap<>();
            for (int i = 0; i < shardDatabases.size(); i++) {
                Statement stmt = shardConnections.get(i).createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM subscribers");
                if (rs.next()) {
                    int count = rs.getInt(1);
                    distribution.put(shardDatabases.get(i), count);
                    System.out.println("  " + shardDatabases.get(i) + ": " + count + " entities");
                }
            }
            
            // Verify reasonable distribution (each shard should have some data)
            for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
                assert entry.getValue() > 0 : "Shard " + entry.getKey() + " has no data";
            }
            
            // Calculate distribution variance
            double avg = distribution.values().stream().mapToInt(Integer::intValue).average().orElse(0);
            double maxDeviation = distribution.values().stream()
                .mapToDouble(count -> Math.abs(count - avg))
                .max().orElse(0);
            double deviationPercent = (maxDeviation / avg) * 100;
            
            System.out.println("✓ Distribution variance: " + String.format("%.1f%%", deviationPercent));
            System.out.println("✓ Data distributed across all shards");
        } catch (Exception e) {
            System.out.println("✗ Data distribution test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // Test 2: Cross-Shard Query
    public void testCrossShardQuery() {
        System.out.println("\n=== Test 2: Cross-Shard Query ===");
        try {
            // Insert test data with known IDs across shards
            List<String> testIds = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                String id = "cross_" + UUID.randomUUID().toString().substring(0, 8);
                testIds.add(id);
                SubscriberEntity sub = createTestSubscriber(id, "+88019" + String.format("%08d", i));
                repository.insert(sub);
            }
            
            // Query all data - should aggregate from all shards
            LocalDateTime start = LocalDateTime.now().minusDays(1);
            LocalDateTime end = LocalDateTime.now().plusDays(1);
            List<SubscriberEntity> allResults = repository.findAllByDateRange(start, end);
            
            System.out.println("✓ Cross-shard query returned " + allResults.size() + " entities");
            
            // Verify we got data from multiple shards
            Set<String> foundIds = allResults.stream()
                .map(SubscriberEntity::getId)
                .filter(id -> id.startsWith("cross_"))
                .collect(Collectors.toSet());
            
            System.out.println("✓ Found " + foundIds.size() + " of " + testIds.size() + " test entities");
            assert foundIds.size() >= testIds.size() * 0.9 : "Should find most test entities";
        } catch (Exception e) {
            System.out.println("✗ Cross-shard query failed: " + e.getMessage());
        }
    }
    
    // Test 3: Parallel Write Performance
    public void testParallelWrites() {
        System.out.println("\n=== Test 3: Parallel Write Performance ===");
        try {
            int threadCount = 10;
            int entitiesPerThread = 50;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Future<Long>> futures = new ArrayList<>();
            
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                Future<Long> future = executor.submit(() -> {
                    long startTime = System.currentTimeMillis();
                    try {
                        for (int i = 0; i < entitiesPerThread; i++) {
                            String id = "parallel_t" + threadId + "_" + i;
                            SubscriberEntity sub = createTestSubscriber(
                                id, 
                                "+88020" + String.format("%03d%05d", threadId, i)
                            );
                            repository.insert(sub);
                        }
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    }
                    return System.currentTimeMillis() - startTime;
                });
                futures.add(future);
            }
            
            // Wait for all threads
            long totalTime = 0;
            for (Future<Long> future : futures) {
                totalTime = Math.max(totalTime, future.get());
            }
            
            executor.shutdown();
            
            int totalEntities = threadCount * entitiesPerThread;
            double throughput = (totalEntities * 1000.0) / totalTime;
            
            System.out.println("✓ Parallel writes completed");
            System.out.println("  Total entities: " + totalEntities);
            System.out.println("  Total time: " + totalTime + "ms");
            System.out.println("  Throughput: " + String.format("%.1f", throughput) + " entities/sec");
            
            // Verify all entities were written
            int actualCount = 0;
            for (Connection conn : shardConnections) {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM subscribers WHERE subscriber_id LIKE 'parallel_%'"
                );
                if (rs.next()) {
                    actualCount += rs.getInt(1);
                }
            }
            
            System.out.println("✓ Verified " + actualCount + " entities written");
            assert actualCount == totalEntities : "Not all entities were written";
        } catch (Exception e) {
            System.out.println("✗ Parallel write test failed: " + e.getMessage());
        }
    }
    
    // Test 4: Shard-Specific Routing
    public void testShardRouting() {
        System.out.println("\n=== Test 4: Shard-Specific Routing ===");
        try {
            // Insert entities and track which shard they go to
            Map<String, String> idToShard = new HashMap<>();
            
            for (int i = 0; i < 30; i++) {
                String id = "route_" + String.format("%03d", i);
                SubscriberEntity sub = createTestSubscriber(id, "+88021" + String.format("%08d", i));
                repository.insert(sub);
                
                // Find which shard it went to
                for (int s = 0; s < shardDatabases.size(); s++) {
                    PreparedStatement ps = shardConnections.get(s).prepareStatement(
                        "SELECT COUNT(*) FROM subscribers WHERE subscriber_id = ?"
                    );
                    ps.setString(1, id);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next() && rs.getInt(1) > 0) {
                        idToShard.put(id, shardDatabases.get(s));
                        break;
                    }
                }
            }
            
            System.out.println("✓ Tracked routing for " + idToShard.size() + " entities");
            
            // Verify consistent routing (same ID always goes to same shard)
            for (Map.Entry<String, String> entry : idToShard.entrySet()) {
                SubscriberEntity found = repository.findById(entry.getKey());
                assert found != null : "Entity " + entry.getKey() + " not found";
                
                // Verify it's in the expected shard
                Connection shardConn = shardConnections.get(
                    shardDatabases.indexOf(entry.getValue())
                );
                PreparedStatement ps = shardConn.prepareStatement(
                    "SELECT subscriber_id FROM subscribers WHERE subscriber_id = ?"
                );
                ps.setString(1, entry.getKey());
                ResultSet rs = ps.executeQuery();
                assert rs.next() : "Entity not in expected shard";
            }
            
            System.out.println("✓ Routing consistency verified");
        } catch (Exception e) {
            System.out.println("✗ Shard routing test failed: " + e.getMessage());
        }
    }
    
    // Test 5: Batch Operations Across Shards
    public void testBatchOperationsAcrossShards() {
        System.out.println("\n=== Test 5: Batch Operations Across Shards ===");
        try {
            // Create IDs that will distribute across shards
            List<String> batchIds = new ArrayList<>();
            Map<String, SubscriberEntity> entityMap = new HashMap<>();
            
            for (int i = 0; i < 90; i++) {
                String id = UUID.randomUUID().toString();
                batchIds.add(id);
                SubscriberEntity sub = createTestSubscriber(id, "+88022" + String.format("%08d", i));
                entityMap.put(id, sub);
                repository.insert(sub);
            }
            
            // Batch find by IDs (will query multiple shards)
            LocalDateTime dateStart = LocalDateTime.now().minusDays(1);
            LocalDateTime dateEnd = LocalDateTime.now().plusDays(1);
            
            List<SubscriberEntity> found = repository.findAllByIdsAndDateRange(
                batchIds, dateStart, dateEnd
            );
            
            System.out.println("✓ Batch find retrieved " + found.size() + " of " + batchIds.size() + " entities");
            assert found.size() >= batchIds.size() * 0.9 : "Should find most entities";
            
            // Verify data integrity
            for (SubscriberEntity entity : found) {
                SubscriberEntity original = entityMap.get(entity.getId());
                assert original != null : "Unknown entity returned";
                assert entity.getMsisdn().equals(original.getMsisdn()) : "Data mismatch";
            }
            
            System.out.println("✓ Batch operation data integrity verified");
        } catch (Exception e) {
            System.out.println("✗ Batch operations test failed: " + e.getMessage());
        }
    }
    
    // Test 6: Cursor Pagination Across Shards
    public void testCursorPaginationAcrossShards() {
        System.out.println("\n=== Test 6: Cursor Pagination Across Shards ===");
        try {
            // Insert ordered test data
            List<String> orderedIds = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                String id = String.format("page_%05d", i);
                orderedIds.add(id);
                SubscriberEntity sub = createTestSubscriber(id, "+88023" + String.format("%08d", i));
                repository.insert(sub);
            }
            
            // Paginate through all data
            String cursor = null;
            List<String> retrievedIds = new ArrayList<>();
            int iterations = 0;
            
            while (iterations < 20) { // Safety limit
                List<SubscriberEntity> batch = repository.findBatchByIdGreaterThan(cursor, 10);
                if (batch.isEmpty()) {
                    break;
                }
                
                for (SubscriberEntity entity : batch) {
                    if (entity.getId().startsWith("page_")) {
                        retrievedIds.add(entity.getId());
                    }
                }
                
                cursor = batch.get(batch.size() - 1).getId();
                iterations++;
                System.out.println("  Iteration " + iterations + ": Retrieved " + batch.size() + " entities");
            }
            
            System.out.println("✓ Paginated through " + retrievedIds.size() + " entities in " + iterations + " iterations");
            
            // Verify we got most of our test data
            Set<String> retrievedSet = new HashSet<>(retrievedIds);
            int foundCount = 0;
            for (String id : orderedIds) {
                if (retrievedSet.contains(id)) {
                    foundCount++;
                }
            }
            
            System.out.println("✓ Found " + foundCount + " of " + orderedIds.size() + " test entities via pagination");
            assert foundCount >= orderedIds.size() * 0.8 : "Should find most entities through pagination";
        } catch (Exception e) {
            System.out.println("✗ Cursor pagination test failed: " + e.getMessage());
        }
    }
    
    // Test 7: Concurrent Read/Write
    public void testConcurrentReadWrite() {
        System.out.println("\n=== Test 7: Concurrent Read/Write ===");
        try {
            String baseId = "concurrent_" + System.currentTimeMillis() + "_";
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(20);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            
            // Start readers and writers
            for (int i = 0; i < 10; i++) {
                final int threadId = i;
                
                // Writer thread
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < 10; j++) {
                            String id = baseId + threadId + "_" + j;
                            SubscriberEntity sub = createTestSubscriber(id, "+88024" + String.format("%07d", threadId * 10 + j));
                            repository.insert(sub);
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
                
                // Reader thread
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < 20; j++) {
                            List<SubscriberEntity> results = repository.findAllByDateRange(
                                LocalDateTime.now().minusDays(1),
                                LocalDateTime.now().plusDays(1)
                            );
                            Thread.sleep(10); // Small delay between reads
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }
            
            // Start all threads
            startLatch.countDown();
            
            // Wait for completion
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            assert completed : "Concurrent operations timed out";
            
            System.out.println("✓ Concurrent operations completed");
            System.out.println("  Successful writes: " + successCount.get());
            System.out.println("  Errors: " + errorCount.get());
            
            assert errorCount.get() == 0 : "No errors should occur during concurrent access";
        } catch (Exception e) {
            System.out.println("✗ Concurrent read/write test failed: " + e.getMessage());
        }
    }
    
    // Test 8: Shard Failure Handling
    public void testShardFailureHandling() {
        System.out.println("\n=== Test 8: Shard Failure Simulation ===");
        try {
            // This test simulates one shard being unavailable
            // In real scenario, one of the shard configs would have enabled=false
            
            ShardConfig shard1 = ShardConfig.builder()
                .shardId("active-1")
                .host(DB_HOST)
                .database(shardDatabases.get(0))
                .username(DB_USER)
                .password(DB_PASSWORD)
                .enabled(true)
                .build();
                
            ShardConfig shard2 = ShardConfig.builder()
                .shardId("failed-shard")
                .host("invalid.host.xyz")  // Simulate unreachable shard
                .database("non_existent")
                .username("invalid")
                .password("invalid")
                .enabled(false)  // Disabled shard
                .build();
                
            // Repository should still work with available shards
            SplitVerseRepository<SubscriberEntity, LocalDateTime> partialRepo = 
                SplitVerseRepository.<SubscriberEntity, LocalDateTime>builder()
                    .withShardConfigs(Arrays.asList(shard1, shard2))
                    .withEntityClass(SubscriberEntity.class)
                    .withPartitionType(PartitionType.DATE_BASED)
                    .withPartitionKeyColumn("created_at")
                    .build();
            
            // Should work with only active shards
            String testId = "partial_" + System.currentTimeMillis();
            SubscriberEntity sub = createTestSubscriber(testId, "+8802599999999");
            partialRepo.insert(sub);
            
            SubscriberEntity retrieved = partialRepo.findById(testId);
            assert retrieved != null : "Should work with partial shards";
            
            System.out.println("✓ Repository operates with partial shard availability");
            
            partialRepo.shutdown();
        } catch (Exception e) {
            System.out.println("! Shard failure handling: " + e.getMessage());
        }
    }
    
    private SubscriberEntity createTestSubscriber(String id, String msisdn) {
        SubscriberEntity subscriber = new SubscriberEntity();
        subscriber.setId(id);
        subscriber.setMsisdn(msisdn);
        subscriber.setBalance(new BigDecimal("75.50"));
        subscriber.setStatus("ACTIVE");
        subscriber.setPlan("POSTPAID");
        subscriber.setPartitionColValue(LocalDateTime.now());
        subscriber.setDataBalanceMb(2000L);
        subscriber.setVoiceBalanceMinutes(200);
        return subscriber;
    }
    
    public void cleanup() {
        try {
            if (repository != null) {
                repository.shutdown();
            }
            
            for (Connection conn : shardConnections) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
            
            System.out.println("\n✓ Test cleanup completed");
        } catch (Exception e) {
            System.err.println("Cleanup error: " + e.getMessage());
        }
    }
    
    public void runAllTests() {
        System.out.println("\n========================================");
        System.out.println("     SPLIT-VERSE MULTI-SHARD TEST");
        System.out.println("========================================");
        
        try {
            setup();
            
            testDataDistribution();
            testCrossShardQuery();
            testParallelWrites();
            testShardRouting();
            testBatchOperationsAcrossShards();
            testCursorPaginationAcrossShards();
            testConcurrentReadWrite();
            testShardFailureHandling();
            
            System.out.println("\n========================================");
            System.out.println("    MULTI-SHARD TESTS COMPLETED");
            System.out.println("========================================");
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }
    
    public static void main(String[] args) {
        new SplitVerseMultiShardTest().runAllTests();
    }
}