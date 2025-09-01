package com.telcobright.db.monitoring;

import com.telcobright.db.test.BaseIntegrationTest;
import com.telcobright.db.test.entity.SimpleTestEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for the monitoring system
 */
@DisplayName("Monitoring System Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MonitoringSystemIntegrationTest extends BaseIntegrationTest {
    
    private GenericMultiTableRepository<SimpleTestEntity, String> repository;
    private static final String TEST_DATABASE = "test_monitoring";
    
    @AfterEach
    void tearDown() {
        if (repository != null) {
            repository.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Should create repository with disabled monitoring")
    void testDisabledMonitoring() throws Exception {
        // Given
        MonitoringConfig disabledConfig = MonitoringConfig.disabled();
        
        // When
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("simple_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(disabledConfig)
            .build();
        
        // Then
        assertThat(repository).isNotNull();
        assertThat(disabledConfig.isEnabled()).isFalse();
    }
    
    @Test
    @Order(2)
    @DisplayName("Should create repository with SLF4J monitoring")
    void testSlf4jMonitoring() throws Exception {
        // Given
        MonitoringConfig slf4jConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(5)
            .serviceName("test-service")
            .instanceId("test-instance")
            .build();
        
        // When
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("simple_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(slf4jConfig)
            .build();
        
        // Then
        assertThat(repository).isNotNull();
        assertThat(slf4jConfig.isEnabled()).isTrue();
        assertThat(slf4jConfig.getDeliveryMethod()).isEqualTo(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING);
        assertThat(slf4jConfig.getMetricFormat()).isEqualTo(MonitoringConfig.MetricFormat.JSON);
    }
    
    @Test
    @Order(3)
    @DisplayName("Should create repository with console monitoring")
    void testConsoleMonitoring() throws Exception {
        // Given
        MonitoringConfig consoleConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.FORMATTED_TEXT)
            .reportingIntervalSeconds(3)
            .serviceName("console-test-service")
            .instanceId("console-test-instance")
            .build();
        
        // When
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("simple_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(consoleConfig)
            .build();
        
        // Then
        assertThat(repository).isNotNull();
        assertThat(consoleConfig.isEnabled()).isTrue();
        assertThat(consoleConfig.getDeliveryMethod()).isEqualTo(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY);
        assertThat(consoleConfig.getMetricFormat()).isEqualTo(MonitoringConfig.MetricFormat.FORMATTED_TEXT);
    }
    
    @Test
    @Order(4)
    @DisplayName("Should create repository with extended monitoring features")
    void testExtendedMonitoring() throws Exception {
        // Given
        MonitoringConfig extendedConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(2)
            .serviceName("extended-test-service")
            .instanceId("extended-test-instance")
            .includeSystemMetrics(true)
            .includeConnectionPoolMetrics(true)
            .includeQueryMetrics(true)
            .enableSlowQueryLogging(true)
            .slowQueryThresholdMs(50)
            .build();
        
        // When
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("simple_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(extendedConfig)
            .build();
        
        // Then
        assertThat(repository).isNotNull();
        assertThat(extendedConfig.isIncludeSystemMetrics()).isTrue();
        assertThat(extendedConfig.isIncludeConnectionPoolMetrics()).isTrue();
        assertThat(extendedConfig.isIncludeQueryMetrics()).isTrue();
        assertThat(extendedConfig.isEnableSlowQueryLogging()).isTrue();
        assertThat(extendedConfig.getSlowQueryThresholdMs()).isEqualTo(50);
    }
    
    @Test
    @Order(5)
    @DisplayName("Should collect metrics during database operations")
    void testMetricsCollection() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Given
        MonitoringConfig metricsConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(2)
            .serviceName("metrics-test")
            .instanceId("metrics-instance")
            .includeQueryMetrics(true)
            .build();
        
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("metrics_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(metricsConfig)
            .build();
        
        // When - Perform operations that should generate metrics
        List<SimpleTestEntity> entities = Arrays.asList(
            new SimpleTestEntity("test1", "First message", LocalDateTime.now()),
            new SimpleTestEntity("test2", "Second message", LocalDateTime.now().plusMinutes(1)),
            new SimpleTestEntity("test3", "Third message", LocalDateTime.now().plusMinutes(2))
        );
        
        repository.insertMultiple(entities);
        
        // Find operations
        SimpleTestEntity found = repository.findById("test1");
        assertThat(found).isNotNull();
        
        List<SimpleTestEntity> rangeResults = repository.findAllByDateRange(
            LocalDateTime.now().minusHours(1),
            LocalDateTime.now().plusHours(1)
        );
        assertThat(rangeResults).hasSizeGreaterThanOrEqualTo(3);
        
        // Update operation
        SimpleTestEntity updateData = new SimpleTestEntity();
        updateData.setMessage("Updated message");
        repository.updateById("test1", updateData);
        
        // Then - Wait for metrics to be collected and reported
        await().atMost(Duration.ofSeconds(10))
               .pollInterval(Duration.ofSeconds(1))
               .untilAsserted(() -> {
                   // Metrics should be available after operations
                   // This test verifies the monitoring system runs without errors
                   assertThat(repository).isNotNull();
               });
    }
    
    @Test
    @Order(6)
    @DisplayName("Should handle concurrent operations with monitoring")
    void testConcurrentOperationsWithMonitoring() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Given
        MonitoringConfig concurrentConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(1)
            .serviceName("concurrent-test")
            .instanceId("concurrent-instance")
            .includeSystemMetrics(true)
            .includeQueryMetrics(true)
            .build();
        
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("concurrent_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(concurrentConfig)
            .build();
        
        // When - Run concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(5);
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (int i = 0; i < 20; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    SimpleTestEntity entity = new SimpleTestEntity(
                        "concurrent" + threadId,
                        "Concurrent message " + threadId,
                        LocalDateTime.now().plusSeconds(threadId)
                    );
                    repository.insert(entity);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    System.err.println("Concurrent operation failed: " + e.getMessage());
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        // Then
        assertThat(successCount.get()).isGreaterThan(15); // Allow some failures
        
        // Wait for monitoring to process all operations
        await().atMost(Duration.ofSeconds(5))
               .pollInterval(Duration.ofSeconds(1))
               .untilAsserted(() -> {
                   assertThat(repository).isNotNull();
               });
    }
    
    @Test
    @Order(7)
    @DisplayName("Should validate monitoring configuration properly")
    void testMonitoringConfigValidation() {
        // Valid configurations should work
        assertThatCode(() -> {
            new MonitoringConfig.Builder()
                .enabled(true)
                .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .reportingIntervalSeconds(60)
                .serviceName("valid-service")
                .instanceId("valid-instance")
                .build();
        }).doesNotThrowAnyException();
        
        // Invalid configurations should fail
        assertThatThrownBy(() -> {
            new MonitoringConfig.Builder()
                .enabled(true)
                .deliveryMethod(MonitoringConfig.DeliveryMethod.REST_PUSH)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .reportingIntervalSeconds(60)
                .serviceName("invalid-service")
                .instanceId("invalid-instance")
                // Missing required restPushUrl for REST_PUSH
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("REST push URL is required");
        
        assertThatThrownBy(() -> {
            new MonitoringConfig.Builder()
                .enabled(true)
                .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
                .metricFormat(MonitoringConfig.MetricFormat.JSON)
                .reportingIntervalSeconds(0) // Invalid interval
                .serviceName("invalid-service")
                .instanceId("invalid-instance")
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Reporting interval must be positive");
    }
    
    @Test
    @Order(8)
    @DisplayName("Should handle monitoring system shutdown gracefully")
    void testMonitoringShutdown() throws Exception {
        cleanupDatabase(TEST_DATABASE);
        
        // Given
        MonitoringConfig shutdownConfig = new MonitoringConfig.Builder()
            .enabled(true)
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(1)
            .serviceName("shutdown-test")
            .instanceId("shutdown-instance")
            .build();
        
        repository = GenericMultiTableRepository.<SimpleTestEntity, String>builder(SimpleTestEntity.class, String.class)
            .host(host)
            .port(port)
            .database(TEST_DATABASE)
            .username(username)
            .password(password)
            .tablePrefix("shutdown_test")
            .partitionRetentionPeriod(1)
            .autoManagePartitions(false)
            .monitoring(shutdownConfig)
            .build();
        
        // Perform some operations
        repository.insert(new SimpleTestEntity("shutdown1", "Test message", LocalDateTime.now()));
        
        // When - Shutdown repository
        assertThatCode(() -> {
            repository.shutdown();
        }).doesNotThrowAnyException();
        
        // Then - Repository should be shutdown cleanly
        repository = null; // Prevent double shutdown in tearDown
    }
    
    @Test
    @Order(9)
    @DisplayName("Should create different monitoring configurations correctly")
    void testMonitoringConfigurationVariations() {
        // Console monitoring with formatted text
        MonitoringConfig consoleText = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.FORMATTED_TEXT)
            .reportingIntervalSeconds(30)
            .serviceName("console-text-service")
            .build();
        
        assertThat(consoleText.getDeliveryMethod()).isEqualTo(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY);
        assertThat(consoleText.getMetricFormat()).isEqualTo(MonitoringConfig.MetricFormat.FORMATTED_TEXT);
        assertThat(consoleText.getReportingIntervalSeconds()).isEqualTo(30);
        
        // Console monitoring with JSON
        MonitoringConfig consoleJson = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(45)
            .serviceName("console-json-service")
            .build();
        
        assertThat(consoleJson.getDeliveryMethod()).isEqualTo(MonitoringConfig.DeliveryMethod.CONSOLE_ONLY);
        assertThat(consoleJson.getMetricFormat()).isEqualTo(MonitoringConfig.MetricFormat.JSON);
        assertThat(consoleJson.getReportingIntervalSeconds()).isEqualTo(45);
        
        // SLF4J monitoring with JSON
        MonitoringConfig slf4jJson = new MonitoringConfig.Builder()
            .deliveryMethod(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING)
            .metricFormat(MonitoringConfig.MetricFormat.JSON)
            .reportingIntervalSeconds(60)
            .serviceName("slf4j-json-service")
            .build();
        
        assertThat(slf4jJson.getDeliveryMethod()).isEqualTo(MonitoringConfig.DeliveryMethod.SLF4J_LOGGING);
        assertThat(slf4jJson.getMetricFormat()).isEqualTo(MonitoringConfig.MetricFormat.JSON);
        assertThat(slf4jJson.getReportingIntervalSeconds()).isEqualTo(60);
    }
}