package com.telcobright.db.maintenance;

import com.telcobright.db.test.BaseIntegrationTest;
import com.telcobright.db.test.entity.SimpleTestEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.monitoring.MonitoringConfig;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for maintenance locking mechanism
 * Tests the ReadWriteLock behavior during partition maintenance
 */
@DisplayName("Maintenance Locking Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MaintenanceLockingIntegrationTest extends BaseIntegrationTest {
    
    private GenericMultiTableRepository<SimpleTestEntity, String> repository;
    private static final String TEST_DATABASE = "test_maintenance";
    
    @BeforeEach
    void setUp() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Create repository with disabled monitoring for cleaner tests
        MonitoringConfig noMonitoring = MonitoringConfig.disabled();
        
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("maintenance_test")
            .partitionRetentionPeriod(2)
            .autoManagePartitions(false)  // Disable automatic scheduler
            .initializePartitionsOnStart(true)
            .monitoring(noMonitoring)
            .build();
    }
    
    @AfterEach
    void tearDown() {
        if (repository != null) {
            repository.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Should allow normal operations when maintenance is not active")
    void testNormalOperationsAllowed() throws Exception {
        // Given
        SimpleTestEntity entity = new SimpleTestEntity("normal1", "Normal message", LocalDateTime.now());
        
        // When/Then - Should work normally
        assertThatCode(() -> {
            repository.insert(entity);
        }).doesNotThrowAnyException();
        
        assertThat(entity.getTestId()).isEqualTo("normal1");
        
        SimpleTestEntity found = repository.findById("normal1");
        assertThat(found).isNotNull();
        assertThat(found.getMessage()).isEqualTo("Normal message");
    }
    
    @Test
    @Order(2)
    @DisplayName("Should block CRUD operations during maintenance")
    void testCrudOperationsBlockedDuringMaintenance() throws Exception {
        // Given - Insert an entity first
        SimpleTestEntity entity = new SimpleTestEntity("blocked1", "To be blocked", LocalDateTime.now());
        repository.insert(entity);
        
        // Use reflection to access the connection provider and simulate maintenance
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            
            // When - Enter maintenance mode
            enterMaintenanceMethod.invoke(connectionProvider, "Test maintenance blocking");
            
            // Then - All CRUD operations should be blocked
            SimpleTestEntity newEntity = new SimpleTestEntity("blocked2", "Should be blocked", LocalDateTime.now());
            assertThatThrownBy(() -> {
                repository.insert(newEntity);
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("Database connections are blocked for maintenance")
              .hasMessageContaining("Test maintenance blocking");
            
            assertThatThrownBy(() -> {
                repository.findById("blocked1");
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("blocked for maintenance");
            
            SimpleTestEntity updateData = new SimpleTestEntity();
            updateData.setMessage("Updated message");
            assertThatThrownBy(() -> {
                repository.updateById("blocked1", updateData);
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("blocked for maintenance");
            
            // Cleanup - Exit maintenance mode
            exitMaintenanceMethod.invoke(connectionProvider);
            
        } catch (Exception e) {
            fail("Failed to test maintenance blocking: " + e.getMessage());
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Should resume normal operations after maintenance ends")
    void testOperationsResumeAfterMaintenance() throws Exception {
        // Given
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            
            // When - Enter and exit maintenance
            enterMaintenanceMethod.invoke(connectionProvider, "Temporary maintenance");
            exitMaintenanceMethod.invoke(connectionProvider);
            
            // Then - Operations should work normally again
            SimpleTestEntity entity = new SimpleTestEntity("resume1", "Resumed operation", LocalDateTime.now());
            assertThatCode(() -> {
                repository.insert(entity);
            }).doesNotThrowAnyException();
            
            SimpleTestEntity found = repository.findById("resume1");
            assertThat(found).isNotNull();
            assertThat(found.getMessage()).isEqualTo("Resumed operation");
            
        } catch (Exception e) {
            fail("Failed to test maintenance resume: " + e.getMessage());
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("Should handle concurrent operations during maintenance correctly")
    void testConcurrentOperationsDuringMaintenance() throws Exception {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch maintenanceStarted = new CountDownLatch(1);
        AtomicInteger blockedOperations = new AtomicInteger(0);
        AtomicInteger successfulOperations = new AtomicInteger(0);
        AtomicReference<Exception> maintenanceError = new AtomicReference<>();
        
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            
            // Start maintenance task
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(100); // Let other threads start
                    enterMaintenanceMethod.invoke(connectionProvider, "Concurrent test maintenance");
                    maintenanceStarted.countDown();
                    Thread.sleep(1000); // Maintenance duration
                    exitMaintenanceMethod.invoke(connectionProvider);
                } catch (Exception e) {
                    maintenanceError.set(e);
                }
            });
            
            // Start worker threads that will attempt operations
            for (int i = 0; i < 15; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        maintenanceStarted.await(5, TimeUnit.SECONDS);
                        
                        SimpleTestEntity entity = new SimpleTestEntity(
                            "concurrent" + threadId,
                            "Concurrent message " + threadId,
                            LocalDateTime.now().plusSeconds(threadId)
                        );
                        
                        repository.insert(entity);
                        successfulOperations.incrementAndGet();
                        
                    } catch (SQLException e) {
                        if (e.getMessage().contains("blocked for maintenance")) {
                            blockedOperations.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Other exceptions
                    }
                });
            }
            
            // When - Start all operations
            startLatch.countDown();
            
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            
            // Then
            assertThat(maintenanceError.get()).isNull();
            assertThat(blockedOperations.get()).isGreaterThan(0); // Some operations should be blocked
            
            // After maintenance ends, operations should succeed
            await().atMost(Duration.ofSeconds(3))
                   .pollInterval(Duration.ofMillis(100))
                   .untilAsserted(() -> {
                       SimpleTestEntity testEntity = new SimpleTestEntity(
                           "after_maintenance",
                           "After maintenance test",
                           LocalDateTime.now()
                       );
                       assertThatCode(() -> repository.insert(testEntity)).doesNotThrowAnyException();
                   });
            
        } catch (Exception e) {
            fail("Failed to test concurrent maintenance: " + e.getMessage());
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("Should handle maintenance during active read operations")
    void testMaintenanceDuringActiveReads() throws Exception {
        // Given - Insert some test data first
        for (int i = 0; i < 10; i++) {
            SimpleTestEntity entity = new SimpleTestEntity(
                "read_test" + i,
                "Read test message " + i,
                LocalDateTime.now().plusMinutes(i)
            );
            repository.insert(entity);
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger readsCompleted = new AtomicInteger(0);
        AtomicInteger readsBlocked = new AtomicInteger(0);
        AtomicBoolean maintenanceActive = new AtomicBoolean(false);
        
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            
            // Start multiple read operations
            for (int i = 0; i < 8; i++) {
                final int readId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        // Try to read data
                        SimpleTestEntity found = repository.findById("read_test" + (readId % 10));
                        if (found != null) {
                            readsCompleted.incrementAndGet();
                        }
                        
                    } catch (SQLException e) {
                        if (e.getMessage().contains("blocked for maintenance")) {
                            readsBlocked.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Other exceptions
                    }
                });
            }
            
            // Start maintenance in the middle of read operations
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(50); // Let some reads start
                    maintenanceActive.set(true);
                    enterMaintenanceMethod.invoke(connectionProvider, "Read blocking test");
                    Thread.sleep(500); // Maintenance duration
                    exitMaintenanceMethod.invoke(connectionProvider);
                    maintenanceActive.set(false);
                } catch (Exception e) {
                    // Maintenance error
                }
            });
            
            // When
            startLatch.countDown();
            
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            
            // Then
            int totalOperations = readsCompleted.get() + readsBlocked.get();
            assertThat(totalOperations).isGreaterThan(0);
            
        } catch (Exception e) {
            fail("Failed to test maintenance during reads: " + e.getMessage());
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("Should provide clear error messages during maintenance")
    void testMaintenanceErrorMessages() throws Exception {
        // Given
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            
            // When
            String maintenanceReason = "Partition cleanup and table optimization";
            enterMaintenanceMethod.invoke(connectionProvider, maintenanceReason);
            
            // Then - Error messages should be informative
            SimpleTestEntity entity = new SimpleTestEntity("error_test", "Error test", LocalDateTime.now());
            
            assertThatThrownBy(() -> {
                repository.insert(entity);
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("Database connections are blocked for maintenance")
              .hasMessageContaining(maintenanceReason);
            
            // Cleanup
            exitMaintenanceMethod.invoke(connectionProvider);
            
        } catch (Exception e) {
            fail("Failed to test error messages: " + e.getMessage());
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("Should handle maintenance state transitions correctly")
    void testMaintenanceStateTransitions() throws Exception {
        // Given
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            var isMaintenanceInProgressMethod = connectionProvider.getClass().getDeclaredMethod("isMaintenanceInProgress");
            isMaintenanceInProgressMethod.setAccessible(true);
            
            // When/Then - Test state transitions
            
            // Initial state - not in maintenance
            boolean initialState = (Boolean) isMaintenanceInProgressMethod.invoke(connectionProvider);
            assertThat(initialState).isFalse();
            
            // Enter maintenance
            enterMaintenanceMethod.invoke(connectionProvider, "State transition test");
            boolean maintenanceState = (Boolean) isMaintenanceInProgressMethod.invoke(connectionProvider);
            assertThat(maintenanceState).isTrue();
            
            // Operations should be blocked
            SimpleTestEntity entity = new SimpleTestEntity("state_test", "State test", LocalDateTime.now());
            assertThatThrownBy(() -> {
                repository.insert(entity);
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("blocked for maintenance");
            
            // Exit maintenance
            exitMaintenanceMethod.invoke(connectionProvider);
            boolean exitState = (Boolean) isMaintenanceInProgressMethod.invoke(connectionProvider);
            assertThat(exitState).isFalse();
            
            // Operations should work again
            assertThatCode(() -> {
                repository.insert(entity);
            }).doesNotThrowAnyException();
            
        } catch (Exception e) {
            fail("Failed to test state transitions: " + e.getMessage());
        }
    }
    
    @Test
    @Order(8)
    @DisplayName("Should handle rapid maintenance state changes")
    void testRapidMaintenanceStateChanges() throws Exception {
        // Given
        try {
            var connectionProviderField = repository.getClass().getDeclaredField("connectionProvider");
            connectionProviderField.setAccessible(true);
            var connectionProvider = connectionProviderField.get(repository);
            
            var enterMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("enterMaintenanceMode", String.class);
            enterMaintenanceMethod.setAccessible(true);
            var exitMaintenanceMethod = connectionProvider.getClass().getDeclaredMethod("exitMaintenanceMode");
            exitMaintenanceMethod.setAccessible(true);
            var isMaintenanceInProgressMethod = connectionProvider.getClass().getDeclaredMethod("isMaintenanceInProgress");
            isMaintenanceInProgressMethod.setAccessible(true);
            
            // When - Rapid state changes
            for (int i = 0; i < 10; i++) {
                enterMaintenanceMethod.invoke(connectionProvider, "Rapid test " + i);
                boolean inMaintenance = (Boolean) isMaintenanceInProgressMethod.invoke(connectionProvider);
                assertThat(inMaintenance).isTrue();
                
                exitMaintenanceMethod.invoke(connectionProvider);
                boolean outMaintenance = (Boolean) isMaintenanceInProgressMethod.invoke(connectionProvider);
                assertThat(outMaintenance).isFalse();
            }
            
            // Then - Final operation should work
            SimpleTestEntity entity = new SimpleTestEntity("rapid_test", "Rapid test", LocalDateTime.now());
            assertThatCode(() -> {
                repository.insert(entity);
            }).doesNotThrowAnyException();
            
        } catch (Exception e) {
            fail("Failed to test rapid state changes: " + e.getMessage());
        }
    }
}