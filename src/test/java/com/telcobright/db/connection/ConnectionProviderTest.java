package com.telcobright.db.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for ConnectionProvider class focusing on maintenance locking
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ConnectionProvider Tests")
class ConnectionProviderTest {
    
    private ConnectionProvider connectionProvider;
    
    @BeforeEach
    void setUp() {
        connectionProvider = new ConnectionProvider(
            "localhost", 3306, "test_db", "user", "password"
        );
    }
    
    @Test
    @DisplayName("Should create ConnectionProvider with correct parameters")
    void testConnectionProviderCreation() {
        // Given/When
        ConnectionProvider provider = new ConnectionProvider(
            "testhost", 3307, "testdb", "testuser", "testpass"
        );
        
        // Then
        assertThat(provider).isNotNull();
        assertThat(provider.isMaintenanceInProgress()).isFalse();
    }
    
    @Test
    @DisplayName("Should initially not be in maintenance mode")
    void testInitialMaintenanceState() {
        // When/Then
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
        assertThat(connectionProvider.getMaintenanceReason()).isEmpty();
    }
    
    @Test
    @DisplayName("Should handle maintenance connection lifecycle")
    void testMaintenanceConnectionLifecycle() {
        // Given
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
        
        // When - Create maintenance connection (will fail without real DB, but tests the mechanism)
        assertThatThrownBy(() -> {
            connectionProvider.getMaintenanceConnection("Test maintenance");
        }).isInstanceOf(SQLException.class)
          .satisfiesAnyOf(
              // MySQL is not running
              exception -> assertThat(exception.getMessage()).contains("Connection refused"),
              // MySQL is running but credentials are invalid
              exception -> assertThat(exception.getMessage()).contains("Access denied")
          );
        
        // Verify maintenance state is reset after failure
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
    }
    
    @Test
    @DisplayName("Should track maintenance state correctly")
    void testMaintenanceStateTracking() {
        // Given
        String reason = "Test maintenance operation";
        
        // Mock scenario: simulate maintenance being active
        try (var maintenance = new MockMaintenanceConnection(connectionProvider, reason)) {
            // Then - During maintenance
            assertThat(connectionProvider.isMaintenanceInProgress()).isTrue();
            assertThat(connectionProvider.getMaintenanceReason()).isEqualTo(reason);
        }
        
        // After maintenance ends
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
        assertThat(connectionProvider.getMaintenanceReason()).isEmpty();
    }
    
    @Test
    @DisplayName("Should provide meaningful maintenance reason")
    void testMaintenanceReason() {
        // Given
        String specificReason = "Partition cleanup and optimization";
        
        // When - Mock maintenance active state
        try (var maintenance = new MockMaintenanceConnection(connectionProvider, specificReason)) {
            // Then
            assertThat(connectionProvider.getMaintenanceReason()).isEqualTo(specificReason);
        }
    }
    
    @Test
    @DisplayName("Should handle builder pattern correctly")
    void testBuilderPattern() {
        // When
        ConnectionProvider provider = new ConnectionProvider.Builder()
            .host("test-host")
            .port(3307)
            .database("test-db")
            .username("test-user")
            .password("test-pass")
            .build();
        
        // Then
        assertThat(provider).isNotNull();
        assertThat(provider.isMaintenanceInProgress()).isFalse();
    }
    
    @Test
    @DisplayName("Should validate builder parameters")
    void testBuilderValidation() {
        // Missing database
        assertThatThrownBy(() -> {
            new ConnectionProvider.Builder()
                .host("test-host")
                .username("test-user")
                .password("test-pass")
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Database, username, and password are required");
        
        // Missing username
        assertThatThrownBy(() -> {
            new ConnectionProvider.Builder()
                .database("test-db")
                .password("test-pass")
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Database, username, and password are required");
        
        // Missing password
        assertThatThrownBy(() -> {
            new ConnectionProvider.Builder()
                .database("test-db")
                .username("test-user")
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Database, username, and password are required");
    }
    
    @Test
    @DisplayName("Should handle shutdown gracefully")
    void testShutdown() {
        // When/Then - Should not throw exception
        assertThatCode(() -> {
            connectionProvider.shutdown();
        }).doesNotThrowAnyException();
    }
    
    @Test
    @DisplayName("Should handle concurrent maintenance attempts")
    @Timeout(10)
    void testConcurrentMaintenanceAttempts() throws InterruptedException {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(5);
        AtomicInteger attemptCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        // When - Multiple threads try to get maintenance connections
        for (int i = 0; i < 10; i++) {
            final int operationId = i;
            executor.submit(() -> {
                try {
                    attemptCount.incrementAndGet();
                    connectionProvider.getMaintenanceConnection("Operation " + operationId);
                    // This should fail due to no real database
                } catch (SQLException e) {
                    failureCount.incrementAndGet();
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        // Then
        assertThat(attemptCount.get()).isEqualTo(10);
        assertThat(failureCount.get()).isEqualTo(10); // All should fail due to no DB
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
    }
    
    @Test
    @DisplayName("Should demonstrate maintenance blocking behavior")
    void testMaintenanceBlockingBehavior() throws InterruptedException {
        // This test demonstrates the expected behavior without requiring a real database
        
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(3);
        AtomicBoolean maintenanceActivated = new AtomicBoolean(false);
        AtomicInteger blockedAttempts = new AtomicInteger(0);
        
        // Simulate maintenance operation
        executor.submit(() -> {
            try {
                // Simulate maintenance connection being held
                maintenanceActivated.set(true);
                try (var maintenance = new MockMaintenanceConnection(connectionProvider, "Simulated maintenance")) {
                    Thread.sleep(500); // Hold maintenance lock
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        // Wait for maintenance to start
        await().atMost(Duration.ofSeconds(2))
               .until(() -> maintenanceActivated.get());
        
        // Attempt normal connections during maintenance
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                try {
                    // This would check if maintenance blocks the operation
                    if (connectionProvider.isMaintenanceInProgress()) {
                        blockedAttempts.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected during testing
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.SECONDS);
        
        // Then
        assertThat(blockedAttempts.get()).isGreaterThan(0);
        assertThat(connectionProvider.isMaintenanceInProgress()).isFalse();
    }
    
    /**
     * Mock maintenance connection for testing without real database
     */
    private static class MockMaintenanceConnection implements AutoCloseable {
        private final ConnectionProvider provider;
        private boolean closed = false;
        
        public MockMaintenanceConnection(ConnectionProvider provider, String reason) {
            this.provider = provider;
            // Simulate maintenance state being activated
            simulateMaintenanceStart(reason);
        }
        
        private void simulateMaintenanceStart(String reason) {
            try {
                var maintenanceField = ConnectionProvider.class.getDeclaredField("maintenanceInProgress");
                maintenanceField.setAccessible(true);
                maintenanceField.setBoolean(provider, true);
                
                var reasonField = ConnectionProvider.class.getDeclaredField("maintenanceReason");
                reasonField.setAccessible(true);
                reasonField.set(provider, reason);
                
                var lockField = ConnectionProvider.class.getDeclaredField("maintenanceLock");
                lockField.setAccessible(true);
                var lock = lockField.get(provider);
                // Acquire write lock
                lock.getClass().getMethod("writeLock").invoke(lock).getClass().getMethod("lock").invoke(lock.getClass().getMethod("writeLock").invoke(lock));
            } catch (Exception e) {
                throw new RuntimeException("Failed to simulate maintenance", e);
            }
        }
        
        @Override
        public void close() {
            if (!closed) {
                closed = true;
                simulateMaintenanceEnd();
            }
        }
        
        private void simulateMaintenanceEnd() {
            try {
                var maintenanceField = ConnectionProvider.class.getDeclaredField("maintenanceInProgress");
                maintenanceField.setAccessible(true);
                maintenanceField.setBoolean(provider, false);
                
                var reasonField = ConnectionProvider.class.getDeclaredField("maintenanceReason");
                reasonField.setAccessible(true);
                reasonField.set(provider, "");
                
                var lockField = ConnectionProvider.class.getDeclaredField("maintenanceLock");
                lockField.setAccessible(true);
                var lock = lockField.get(provider);
                // Release write lock
                lock.getClass().getMethod("writeLock").invoke(lock).getClass().getMethod("unlock").invoke(lock.getClass().getMethod("writeLock").invoke(lock));
            } catch (Exception e) {
                throw new RuntimeException("Failed to simulate maintenance end", e);
            }
        }
    }
}