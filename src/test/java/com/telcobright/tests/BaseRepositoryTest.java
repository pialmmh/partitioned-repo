package com.telcobright.tests;

import com.telcobright.core.connection.ConnectionProvider;
import com.telcobright.core.logging.ConsoleLogger;
import com.telcobright.core.logging.Logger;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base test class providing common infrastructure for all repository tests
 */
public abstract class BaseRepositoryTest {
    
    protected static final String TEST_HOST = "127.0.0.1";
    protected static final int TEST_PORT = 3306;
    protected static final String TEST_USER = "root";
    protected static final String TEST_PASSWORD = "123456";
    protected static final String TEST_DATABASE = "test";
    protected static final String BACKUP_DATABASE = "test_backup";
    
    protected final Logger logger = new ConsoleLogger();
    protected ConnectionProvider connectionProvider;
    protected Random random = new Random();
    protected DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    // Test metrics
    protected final AtomicLong totalOperations = new AtomicLong(0);
    protected final AtomicLong successfulOperations = new AtomicLong(0);
    protected final AtomicLong failedOperations = new AtomicLong(0);
    protected final AtomicLong totalLatencyMs = new AtomicLong(0);
    protected final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    
    // Test control
    protected volatile boolean testRunning = false;
    protected ExecutorService executorService;
    protected ScheduledExecutorService scheduledExecutor;
    
    /**
     * Setup test environment
     */
    protected void setUp() throws SQLException {
        // Initialize connection provider
        connectionProvider = new ConnectionProvider.Builder()
            .host(TEST_HOST)
            .port(TEST_PORT)
            .database(TEST_DATABASE)
            .username(TEST_USER)
            .password(TEST_PASSWORD)
            .maxConnections(50)
            .build();
        
        // Create thread pools
        executorService = Executors.newFixedThreadPool(16);
        scheduledExecutor = Executors.newScheduledThreadPool(4);
        
        // Clean test database
        cleanDatabase();
        
        // Create test schema
        createTestSchema();
        
        logger.info("Test environment initialized");
    }
    
    /**
     * Tear down test environment
     */
    protected void tearDown() throws SQLException {
        testRunning = false;
        
        // Shutdown executors
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
        
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
        
        // Clean database
        cleanDatabase();
        
        // Close connections
        if (connectionProvider != null) {
            connectionProvider.shutdown();
        }
        
        logger.info("Test environment cleaned up");
    }
    
    /**
     * Clean test database
     */
    protected void cleanDatabase() throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Drop all test tables
            ResultSet rs = stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = '" + TEST_DATABASE + "' " +
                "AND (table_name LIKE 'test_%' OR table_name LIKE 'sms_%' OR table_name LIKE 'orders%')"
            );
            
            List<String> tablesToDrop = new ArrayList<>();
            while (rs.next()) {
                tablesToDrop.add(rs.getString("table_name"));
            }
            
            for (String table : tablesToDrop) {
                stmt.execute("DROP TABLE IF EXISTS " + TEST_DATABASE + "." + table);
                logger.info("Dropped table: " + table);
            }
        }
    }
    
    /**
     * Create test schema and tables
     */
    protected void createTestSchema() throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create test tracking tables
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS test_metrics (" +
                "  id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                "  test_name VARCHAR(100)," +
                "  metric_name VARCHAR(50)," +
                "  metric_value DECIMAL(20,3)," +
                "  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "  KEY idx_test_timestamp (test_name, timestamp)" +
                ") ENGINE=InnoDB"
            );
            
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS test_results (" +
                "  test_id VARCHAR(50) PRIMARY KEY," +
                "  test_name VARCHAR(100)," +
                "  start_time TIMESTAMP," +
                "  end_time TIMESTAMP," +
                "  total_operations BIGINT," +
                "  successful_operations BIGINT," +
                "  failed_operations BIGINT," +
                "  avg_latency_ms DECIMAL(10,3)," +
                "  p95_latency_ms DECIMAL(10,3)," +
                "  p99_latency_ms DECIMAL(10,3)," +
                "  status VARCHAR(20)," +
                "  notes TEXT" +
                ") ENGINE=InnoDB"
            );
            
            logger.info("Test schema created");
        }
    }
    
    /**
     * Record test metric
     */
    protected void recordMetric(String testName, String metricName, double value) {
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO test_metrics (test_name, metric_name, metric_value) VALUES (?, ?, ?)"
             )) {
            
            stmt.setString(1, testName);
            stmt.setString(2, metricName);
            stmt.setDouble(3, value);
            stmt.executeUpdate();
            
        } catch (SQLException e) {
            logger.error("Failed to record metric: " + e.getMessage());
        }
    }
    
    /**
     * Record operation latency
     */
    protected void recordLatency(long latencyMs) {
        totalOperations.incrementAndGet();
        totalLatencyMs.addAndGet(latencyMs);
        latencies.add(latencyMs);
        
        // Keep only last 10000 latencies for percentile calculation
        if (latencies.size() > 10000) {
            synchronized (latencies) {
                if (latencies.size() > 10000) {
                    latencies.subList(0, latencies.size() - 10000).clear();
                }
            }
        }
    }
    
    /**
     * Calculate percentile from latencies
     */
    protected long calculatePercentile(double percentile) {
        if (latencies.isEmpty()) {
            return 0;
        }
        
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
    
    /**
     * Save test results
     */
    protected void saveTestResults(String testId, String testName, long startTime, long endTime, String status) {
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO test_results " +
                 "(test_id, test_name, start_time, end_time, total_operations, " +
                 "successful_operations, failed_operations, avg_latency_ms, " +
                 "p95_latency_ms, p99_latency_ms, status) " +
                 "VALUES (?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?, ?, ?, ?, ?, ?)"
             )) {
            
            long avgLatency = totalOperations.get() > 0 ? 
                totalLatencyMs.get() / totalOperations.get() : 0;
            
            stmt.setString(1, testId);
            stmt.setString(2, testName);
            stmt.setLong(3, startTime / 1000);
            stmt.setLong(4, endTime / 1000);
            stmt.setLong(5, totalOperations.get());
            stmt.setLong(6, successfulOperations.get());
            stmt.setLong(7, failedOperations.get());
            stmt.setLong(8, avgLatency);
            stmt.setLong(9, calculatePercentile(95));
            stmt.setLong(10, calculatePercentile(99));
            stmt.setString(11, status);
            
            stmt.executeUpdate();
            
            logger.info("Test results saved: " + testId);
            
        } catch (SQLException e) {
            logger.error("Failed to save test results: " + e.getMessage());
        }
    }
    
    /**
     * Execute query and measure performance
     */
    protected <T> T executeTimedQuery(String sql, QueryExecutor<T> executor) throws SQLException {
        long startTime = System.currentTimeMillis();
        
        try (Connection conn = connectionProvider.getConnection()) {
            T result = executor.execute(conn, sql);
            
            long latency = System.currentTimeMillis() - startTime;
            recordLatency(latency);
            successfulOperations.incrementAndGet();
            
            return result;
            
        } catch (SQLException e) {
            failedOperations.incrementAndGet();
            throw e;
        }
    }
    
    /**
     * Execute update and measure performance
     */
    protected int executeTimedUpdate(String sql, Object... params) throws SQLException {
        long startTime = System.currentTimeMillis();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            
            int result = stmt.executeUpdate();
            
            long latency = System.currentTimeMillis() - startTime;
            recordLatency(latency);
            successfulOperations.incrementAndGet();
            
            return result;
            
        } catch (SQLException e) {
            failedOperations.incrementAndGet();
            throw e;
        }
    }
    
    /**
     * Verify data integrity
     */
    protected void verifyDataIntegrity(String tableName) throws SQLException {
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Check for duplicate IDs
            ResultSet rs = stmt.executeQuery(
                "SELECT id, COUNT(*) as count FROM " + tableName + 
                " GROUP BY id HAVING COUNT(*) > 1"
            );
            
            if (rs.next()) {
                throw new AssertionError("Duplicate IDs found in table: " + tableName);
            }
            
            // Check for null values in required fields
            rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM " + tableName + 
                " WHERE id IS NULL OR created_at IS NULL"
            );
            
            rs.next();
            if (rs.getInt(1) > 0) {
                throw new AssertionError("Null values found in required fields");
            }
            
            logger.info("Data integrity verified for table: " + tableName);
        }
    }
    
    /**
     * Generate test data
     */
    protected Map<String, Object> generateTestData() {
        Map<String, Object> data = new HashMap<>();
        data.put("customer_id", random.nextInt(10000) + 1);
        data.put("amount", random.nextDouble() * 1000);
        data.put("status", randomStatus());
        data.put("created_at", randomTimestamp());
        return data;
    }
    
    /**
     * Generate random status
     */
    protected String randomStatus() {
        String[] statuses = {"pending", "processing", "completed", "failed"};
        return statuses[random.nextInt(statuses.length)];
    }
    
    /**
     * Generate random timestamp within range
     */
    protected LocalDateTime randomTimestamp() {
        return LocalDateTime.now()
            .minusDays(random.nextInt(30))
            .minusHours(random.nextInt(24))
            .minusMinutes(random.nextInt(60));
    }
    
    /**
     * Print test summary
     */
    protected void printTestSummary() {
        logger.info("==== TEST SUMMARY ====");
        logger.info("Total Operations: " + totalOperations.get());
        logger.info("Successful: " + successfulOperations.get());
        logger.info("Failed: " + failedOperations.get());
        
        if (totalOperations.get() > 0) {
            logger.info("Average Latency: " + (totalLatencyMs.get() / totalOperations.get()) + " ms");
            logger.info("P95 Latency: " + calculatePercentile(95) + " ms");
            logger.info("P99 Latency: " + calculatePercentile(99) + " ms");
            logger.info("Success Rate: " + 
                String.format("%.2f%%", successfulOperations.get() * 100.0 / totalOperations.get()));
        }
        logger.info("=====================");
    }
    
    /**
     * Functional interface for query execution
     */
    @FunctionalInterface
    protected interface QueryExecutor<T> {
        T execute(Connection conn, String sql) throws SQLException;
    }
    
    /**
     * Abstract method to be implemented by specific test classes
     */
    public abstract void runAllTests() throws Exception;
}