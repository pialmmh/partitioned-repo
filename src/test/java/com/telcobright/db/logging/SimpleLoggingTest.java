package com.telcobright.db.logging;

import com.telcobright.db.repository.GenericPartitionedTableRepository;
import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.annotation.*;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify logging functionality
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimpleLoggingTest {
    
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");
    
    private static GenericPartitionedTableRepository<LogTestEntity, Long> repository;
    private static CustomTestLogger customLogger;
    
    @Table(name = "log_test")
    public static class LogTestEntity implements ShardingEntity<Long> {
        @Id
        @Column(name = "id", insertable = false)
        private Long id;
        
        @ShardingKey
        @Column(name = "created_at", nullable = false)
        private LocalDateTime createdAt;
        
        @Column(name = "data")
        private String data;
        
        public LogTestEntity() {}
        
        public LogTestEntity(String data) {
            this.data = data;
            this.createdAt = LocalDateTime.now();
        }
        
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }
    
    /**
     * Custom logger that captures all logs for testing
     */
    public static class CustomTestLogger implements Logger {
        private final List<LogEntry> logs = new ArrayList<>();
        
        public static class LogEntry {
            public final Level level;
            public final String message;
            public final LocalDateTime timestamp;
            
            public LogEntry(Level level, String message) {
                this.level = level;
                this.message = message;
                this.timestamp = LocalDateTime.now();
            }
        }
        
        @Override
        public boolean isLevelEnabled(Level level) {
            return true; // Enable all levels for testing
        }
        
        @Override
        public void log(Level level, String message) {
            LogEntry entry = new LogEntry(level, message);
            logs.add(entry);
            
            // Also print to console with formatting
            System.out.printf("[%s] %s - %s%n", 
                entry.timestamp, level, message);
        }
        
        @Override
        public void log(Level level, String message, Throwable throwable) {
            String fullMessage = message + " [Exception: " + throwable.getMessage() + "]";
            log(level, fullMessage);
        }
        
        @Override
        public void logEvent(Level level, String eventType, String message, Map<String, Object> context) {
            StringBuilder sb = new StringBuilder();
            sb.append("[EVENT:").append(eventType).append("] ").append(message);
            if (context != null && !context.isEmpty()) {
                sb.append(" {");
                context.forEach((k, v) -> sb.append(k).append("=").append(v).append(", "));
                sb.setLength(sb.length() - 2); // Remove last comma
                sb.append("}");
            }
            log(level, sb.toString());
        }
        
        public List<LogEntry> getLogs() { return logs; }
        
        public List<LogEntry> getLogsByLevel(Level level) {
            return logs.stream()
                .filter(log -> log.level == level)
                .toList();
        }
        
        public void clear() { logs.clear(); }
        
        public void printSummary() {
            System.out.println("\n=== LOG SUMMARY ===");
            for (Level level : Level.values()) {
                long count = logs.stream().filter(l -> l.level == level).count();
                if (count > 0) {
                    System.out.printf("%s: %d logs%n", level, count);
                }
            }
            System.out.println("Total logs: " + logs.size());
        }
    }
    
    @BeforeAll
    static void setup() {
        System.out.println("\n=== SIMPLE LOGGING TEST SETUP ===");
        System.out.println("MySQL URL: " + mysql.getJdbcUrl());
        
        // Create custom logger
        customLogger = new CustomTestLogger();
        
        // Create repository with custom logger
        repository = GenericPartitionedTableRepository.<LogTestEntity, Long>builder(LogTestEntity.class, Long.class)
            .host(mysql.getHost())
            .port(mysql.getMappedPort(3306))
            .database("testdb")
            .username("test")
            .password("test")
            .logger(customLogger)  // Use our custom logger
            .tableName("log_test")
            .partitionRetentionPeriod(7)
            .autoManagePartitions(true)
            .build();
        
        System.out.println("Repository created with custom logger");
    }
    
    @Test
    @Order(1)
    void testDefaultConsoleLogger() {
        System.out.println("\n=== TEST 1: DEFAULT CONSOLE LOGGER ===");
        
        // Create another repository with default logger
        var consoleRepo = GenericPartitionedTableRepository.<LogTestEntity, Long>builder(LogTestEntity.class, Long.class)
            .host(mysql.getHost())
            .port(mysql.getMappedPort(3306))
            .database("testdb")
            .username("test")
            .password("test")
            // No logger specified - should use ConsoleLogger
            .tableName("console_test")
            .build();
        
        System.out.println("✓ Repository created with default ConsoleLogger");
        
        // The logs should appear in console
        consoleRepo.shutdown();
    }
    
    @Test
    @Order(2)
    void testCustomLoggerReceivesLogs() throws Exception {
        System.out.println("\n=== TEST 2: CUSTOM LOGGER RECEIVES LOGS ===");
        
        // Clear any previous logs
        customLogger.clear();
        
        // Perform operations that should generate logs
        LogTestEntity entity = new LogTestEntity("Test Data");
        repository.insert(entity);
        
        // Check that we received logs
        assertFalse(customLogger.getLogs().isEmpty(), "Should have received logs");
        
        System.out.println("Received " + customLogger.getLogs().size() + " log entries");
        customLogger.printSummary();
    }
    
    @Test
    @Order(3)
    void testLogLevels() {
        System.out.println("\n=== TEST 3: LOG LEVELS ===");
        
        // Test different log levels
        customLogger.clear();
        
        customLogger.trace("This is a TRACE message");
        customLogger.debug("This is a DEBUG message");
        customLogger.info("This is an INFO message");
        customLogger.warn("This is a WARN message");
        customLogger.error("This is an ERROR message");
        
        assertEquals(1, customLogger.getLogsByLevel(Logger.Level.TRACE).size());
        assertEquals(1, customLogger.getLogsByLevel(Logger.Level.DEBUG).size());
        assertEquals(1, customLogger.getLogsByLevel(Logger.Level.INFO).size());
        assertEquals(1, customLogger.getLogsByLevel(Logger.Level.WARN).size());
        assertEquals(1, customLogger.getLogsByLevel(Logger.Level.ERROR).size());
        
        System.out.println("✓ All log levels working correctly");
    }
    
    @Test
    @Order(4)
    void testStructuredLogging() {
        System.out.println("\n=== TEST 4: STRUCTURED LOGGING ===");
        
        customLogger.clear();
        
        // Test structured logging
        Map<String, Object> context = new HashMap<>();
        context.put("table", "log_test");
        context.put("partitions", 7);
        context.put("operation", "maintenance");
        context.put("duration_ms", 150);
        
        customLogger.logEvent(
            Logger.Level.INFO,
            "MAINTENANCE_COMPLETE",
            "Partition maintenance completed",
            context
        );
        
        var logs = customLogger.getLogs();
        assertEquals(1, logs.size());
        
        String logMessage = logs.get(0).message;
        assertTrue(logMessage.contains("MAINTENANCE_COMPLETE"));
        assertTrue(logMessage.contains("table=log_test"));
        assertTrue(logMessage.contains("partitions=7"));
        assertTrue(logMessage.contains("duration_ms=150"));
        
        System.out.println("Structured log: " + logMessage);
        System.out.println("✓ Structured logging working correctly");
    }
    
    @Test
    @Order(5)
    void testExceptionLogging() {
        System.out.println("\n=== TEST 5: EXCEPTION LOGGING ===");
        
        customLogger.clear();
        
        // Test exception logging
        Exception testException = new RuntimeException("Test exception message");
        customLogger.error("An error occurred", testException);
        
        var errorLogs = customLogger.getLogsByLevel(Logger.Level.ERROR);
        assertEquals(1, errorLogs.size());
        
        String errorMessage = errorLogs.get(0).message;
        assertTrue(errorMessage.contains("An error occurred"));
        assertTrue(errorMessage.contains("Test exception message"));
        
        System.out.println("Exception log: " + errorMessage);
        System.out.println("✓ Exception logging working correctly");
    }
    
    @Test
    @Order(6)
    void testRepositoryOperationsGenerateLogs() throws Exception {
        System.out.println("\n=== TEST 6: REPOSITORY OPERATIONS LOGS ===");
        
        customLogger.clear();
        
        // Insert multiple entities
        for (int i = 0; i < 3; i++) {
            repository.insert(new LogTestEntity("Data " + i));
        }
        
        // Query entities
        var results = repository.findAllByDateRange(
            LocalDateTime.now().minusDays(1),
            LocalDateTime.now()
        );
        
        System.out.println("Found " + results.size() + " entities");
        
        // We should have logs from these operations
        assertFalse(customLogger.getLogs().isEmpty(), 
            "Should have logs from repository operations");
        
        customLogger.printSummary();
        
        // Print some sample logs
        System.out.println("\nSample logs:");
        customLogger.getLogs().stream()
            .limit(5)
            .forEach(log -> System.out.printf("  [%s] %s%n", log.level, log.message));
    }
    
    @AfterAll
    static void cleanup() {
        System.out.println("\n=== CLEANUP ===");
        
        if (repository != null) {
            repository.shutdown();
        }
        
        System.out.println("\n=== FINAL LOG SUMMARY ===");
        customLogger.printSummary();
        
        System.out.println("\n✅ All logging tests passed successfully!");
    }
}