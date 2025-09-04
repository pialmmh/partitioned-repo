package com.telcobright.core.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of MonitoringService
 */
public class DefaultMonitoringService implements MonitoringService {
    
    private final MonitoringConfig config;
    private final RepositoryMetrics metrics;
    private final MetricsCollector metricsCollector;
    // private final ObjectMapper objectMapper; // Removed - Jackson dependency
    private final HttpClient httpClient;
    
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> monitoringTask;
    
    public DefaultMonitoringService(MonitoringConfig config, RepositoryMetrics metrics, MetricsCollector metricsCollector) {
        this.config = config;
        this.metrics = metrics;
        this.metricsCollector = metricsCollector;
        // this.objectMapper = new ObjectMapper(); // Removed - Jackson dependency
        // this.objectMapper.registerModule(new JavaTimeModule()); // Removed - Jackson dependency
        this.httpClient = HttpClient.newHttpClient();
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "repository-monitoring");
            t.setDaemon(true);
            return t;
        });
    }
    
    @Override
    public void start() {
        if (!config.isEnabled()) {
            return;
        }
        
        if (running.compareAndSet(false, true)) {
            // Print initial metrics immediately
            System.out.printf("Started repository monitoring: %s%n", config);
            recordInitialMetrics();
            
            // Schedule periodic reporting
            monitoringTask = scheduler.scheduleAtFixedRate(
                this::reportMetrics,
                config.getReportingIntervalSeconds(),
                config.getReportingIntervalSeconds(),
                TimeUnit.SECONDS
            );
        }
    }
    
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (monitoringTask != null) {
                monitoringTask.cancel(false);
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    @Override
    public boolean isRunning() {
        return running.get();
    }
    
    @Override
    public RepositoryMetrics getMetrics() {
        return metrics;
    }
    
    @Override
    public void recordMaintenanceJobStart() {
        metrics.setLastRunTimestamp(LocalDateTime.now());
        // TODO: Set next scheduled time based on scheduler configuration
    }
    
    @Override
    public void recordMaintenanceJobSuccess(long durationMs, int partitionsCreated, int partitionsDeleted) {
        metrics.setLastJobDurationMs(durationMs);
        metrics.setPartitionsCreatedLastRun(partitionsCreated);
        metrics.setPartitionsDeletedLastRun(partitionsDeleted);
        metrics.setLastMaintenanceJobSucceeded(true);
        
        // Collect current partition info
        collectPartitionMetrics();
    }
    
    @Override
    public void recordMaintenanceJobFailure(long durationMs, Throwable error) {
        metrics.setLastJobDurationMs(durationMs);
        metrics.setLastMaintenanceJobSucceeded(false);
        
        System.err.printf("Maintenance job failed after %d ms: %s%n", durationMs, error.getMessage());
    }
    
    @Override
    public void updatePartitionInfo(int partitionCount, int prefixedTableCount) {
        metrics.setNumberOfPartitions(partitionCount);
        metrics.setNumberOfPrefixedTables(prefixedTableCount);
    }
    
    @Override
    public MonitoringConfig getConfig() {
        return config;
    }
    
    @Override
    public String getPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();
        
        // Scheduler metrics
        sb.append("# HELP repository_last_run_timestamp_seconds Last maintenance job run timestamp\n");
        sb.append("# TYPE repository_last_run_timestamp_seconds gauge\n");
        sb.append(String.format("repository_last_run_timestamp_seconds{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getLastRunTimestampEpoch()));
        
        sb.append("# HELP repository_last_job_duration_seconds Last maintenance job duration\n");
        sb.append("# TYPE repository_last_job_duration_seconds gauge\n");
        sb.append(String.format("repository_last_job_duration_seconds{repository_type=\"%s\",repository_name=\"%s\"} %.3f%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getLastJobDurationMs() / 1000.0));
        
        // Partition metrics
        sb.append("# HELP repository_partitions_total Total number of partitions\n");
        sb.append("# TYPE repository_partitions_total gauge\n");
        sb.append(String.format("repository_partitions_total{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getNumberOfPartitions()));
        
        sb.append("# HELP repository_prefixed_tables_total Total number of prefixed tables\n");
        sb.append("# TYPE repository_prefixed_tables_total gauge\n");
        sb.append(String.format("repository_prefixed_tables_total{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getNumberOfPrefixedTables()));
        
        sb.append("# HELP repository_partitions_created_last_run Partitions created in last run\n");
        sb.append("# TYPE repository_partitions_created_last_run gauge\n");
        sb.append(String.format("repository_partitions_created_last_run{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getPartitionsCreatedLastRun()));
        
        sb.append("# HELP repository_partitions_deleted_last_run Partitions deleted in last run\n");
        sb.append("# TYPE repository_partitions_deleted_last_run gauge\n");
        sb.append(String.format("repository_partitions_deleted_last_run{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getPartitionsDeletedLastRun()));
        
        // Error metrics
        sb.append("# HELP repository_last_job_success Last job success status (1=success, 0=failure)\n");
        sb.append("# TYPE repository_last_job_success gauge\n");
        sb.append(String.format("repository_last_job_success{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.isLastMaintenanceJobSucceeded() ? 1 : 0));
        
        sb.append("# HELP repository_total_failures_total Total number of maintenance job failures\n");
        sb.append("# TYPE repository_total_failures_total counter\n");
        sb.append(String.format("repository_total_failures_total{repository_type=\"%s\",repository_name=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getTotalFailureCount()));
        
        // Service info
        sb.append("# HELP repository_startup_timestamp_seconds Service startup timestamp\n");
        sb.append("# TYPE repository_startup_timestamp_seconds gauge\n");
        sb.append(String.format("repository_startup_timestamp_seconds{repository_type=\"%s\",repository_name=\"%s\",hostname=\"%s\"} %d%n",
                metrics.getRepositoryType(), metrics.getRepositoryName(), metrics.getHostname(), metrics.getStartupTimestampEpoch()));
        
        return sb.toString();
    }
    
    @Override
    public String getJsonMetrics() {
        // JSON functionality disabled - Jackson dependency not available
        return "{\"error\":\"Jackson dependency not available\"}";
        /*
        try {
            ObjectNode json = objectMapper.createObjectNode();
            
            // Service info
            json.put("service_name", config.getServiceName());
            json.put("instance_id", config.getInstanceId());
            json.put("hostname", metrics.getHostname());
            json.put("startup_timestamp", metrics.getStartupTimestamp().toString());
            json.put("startup_timestamp_epoch", metrics.getStartupTimestampEpoch());
            
            // Repository info
            json.put("repository_type", metrics.getRepositoryType());
            json.put("repository_name", metrics.getRepositoryName());
            
            // Scheduler metrics
            ObjectNode scheduler = json.putObject("scheduler");
            if (metrics.getLastRunTimestamp() != null) {
                scheduler.put("last_run_timestamp", metrics.getLastRunTimestamp().toString());
                scheduler.put("last_run_timestamp_epoch", metrics.getLastRunTimestampEpoch());
            }
            if (metrics.getNextScheduledTime() != null) {
                scheduler.put("next_scheduled_time", metrics.getNextScheduledTime().toString());
                scheduler.put("next_scheduled_time_epoch", metrics.getNextScheduledTimeEpoch());
            }
            scheduler.put("last_job_duration_ms", metrics.getLastJobDurationMs());
            
            // Partition metrics
            ObjectNode partitions = json.putObject("partition_maintenance");
            partitions.put("number_of_partitions", metrics.getNumberOfPartitions());
            partitions.put("number_of_prefixed_tables", metrics.getNumberOfPrefixedTables());
            partitions.put("partitions_created_last_run", metrics.getPartitionsCreatedLastRun());
            partitions.put("partitions_deleted_last_run", metrics.getPartitionsDeletedLastRun());
            
            if (metrics.getTotalListOfPartitions() != null) {
                partitions.set("total_list_of_partitions", objectMapper.valueToTree(metrics.getTotalListOfPartitions()));
            }
            if (metrics.getNextPartitionsToDelete() != null) {
                partitions.set("next_partitions_to_delete", objectMapper.valueToTree(metrics.getNextPartitionsToDelete()));
            }
            
            // Error metrics
            ObjectNode errors = json.putObject("errors");
            errors.put("last_maintenance_job_succeeded", metrics.isLastMaintenanceJobSucceeded());
            errors.put("total_failure_count", metrics.getTotalFailureCount());
            
            return objectMapper.writeValueAsString(json);
        } catch (Exception e) {
            return String.format("{\"error\": \"Failed to serialize metrics: %s\"}", e.getMessage());
        }
        */
    }
    
    private void reportMetrics() {
        try {
            // Collect latest partition info before reporting
            collectPartitionMetrics();
            
            // Always print simplified metrics to console first
            printSimplifiedMetrics();
            
            // Then handle configured delivery method
            switch (config.getDeliveryMethod()) {
                case HTTP_ENDPOINT:
                    // HTTP endpoint doesn't push - metrics are pulled via getPrometheusMetrics()
                    break;
                case REST_PUSH:
                    pushToRestEndpoint();
                    break;
                case SLF4J_LOGGING:
                    logMetrics();
                    break;
                case KAFKA_PUSH:
                    // TODO: Implement Kafka push
                    break;
            }
        } catch (Exception e) {
            System.err.printf("Error reporting metrics: %s%n", e.getMessage());
        }
    }
    
    private void collectPartitionMetrics() {
        if (metricsCollector != null) {
            try {
                String tableName = determineMainTableName();
                if (tableName != null) {
                    int partitionCount = metricsCollector.countPartitions(tableName);
                    int prefixedTableCount = metricsCollector.countPrefixedTables(metrics.getRepositoryName());
                    
                    metrics.setNumberOfPartitions(partitionCount);
                    metrics.setNumberOfPrefixedTables(prefixedTableCount);
                    
                    // Get partition names
                    metrics.setTotalListOfPartitions(metricsCollector.getPartitionNames(tableName));
                }
            } catch (Exception e) {
                System.err.printf("Error collecting partition metrics: %s%n", e.getMessage());
            }
        }
    }
    
    private String determineMainTableName() {
        // For partitioned repos, the main table name is typically the repository name
        // For multi-table repos, we might need to look at the most recent table
        return metrics.getRepositoryName();
    }
    
    private void pushToRestEndpoint() {
        try {
            String jsonPayload = getJsonMetrics();
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(config.getRestPushUrl()))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() >= 400) {
                System.err.printf("Failed to push metrics to %s: HTTP %d - %s%n",
                        config.getRestPushUrl(), response.statusCode(), response.body());
            }
        } catch (Exception e) {
            System.err.printf("Error pushing metrics to REST endpoint: %s%n", e.getMessage());
        }
    }
    
    private void logMetrics() {
        String jsonMetrics = getJsonMetrics();
        System.out.printf("REPOSITORY_METRICS: %s%n", jsonMetrics);
    }
    
    /**
     * Always print simplified, human-readable metrics to console
     */
    private void printSimplifiedMetrics() {
        System.out.println();
        System.out.println("========================================");
        System.out.printf(" REPOSITORY METRICS - %s ", java.time.LocalDateTime.now().toString());
        System.out.println();
        System.out.println("========================================");
        
        // Service Info
        System.out.printf("Service: %s (%s)%n", config.getServiceName(), config.getInstanceId());
        System.out.printf("Repository: %s [%s]%n", metrics.getRepositoryName(), metrics.getRepositoryType());
        System.out.printf("Hostname: %s%n", metrics.getHostname());
        System.out.println();
        
        // Scheduler Status
        System.out.println("📅 SCHEDULER STATUS:");
        if (metrics.getLastRunTimestamp() != null) {
            System.out.printf("   Last Run: %s%n", metrics.getLastRunTimestamp().toString());
            System.out.printf("   Duration: %d ms%n", metrics.getLastJobDurationMs());
        } else {
            System.out.println("   Last Run: Never");
        }
        
        if (metrics.getNextScheduledTime() != null) {
            System.out.printf("   Next Run: %s%n", metrics.getNextScheduledTime().toString());
        }
        System.out.println();
        
        // Partition Metrics
        System.out.println("📊 PARTITION METRICS:");
        System.out.printf("   Total Partitions: %d%n", metrics.getNumberOfPartitions());
        System.out.printf("   Prefixed Tables: %d%n", metrics.getNumberOfPrefixedTables());
        System.out.printf("   Created Last Run: %d%n", metrics.getPartitionsCreatedLastRun());
        System.out.printf("   Deleted Last Run: %d%n", metrics.getPartitionsDeletedLastRun());
        
        if (metrics.getTotalListOfPartitions() != null && !metrics.getTotalListOfPartitions().isEmpty()) {
            System.out.printf("   Active Partitions: %s%n", 
                            metrics.getTotalListOfPartitions().size() > 5 
                                ? metrics.getTotalListOfPartitions().subList(0, 5) + "... (+" + (metrics.getTotalListOfPartitions().size() - 5) + " more)"
                                : metrics.getTotalListOfPartitions());
        }
        System.out.println();
        
        // Health Status
        System.out.println("🏥 HEALTH STATUS:");
        String healthStatus = metrics.isLastMaintenanceJobSucceeded() ? "✅ HEALTHY" : "❌ FAILED";
        System.out.printf("   Last Job: %s%n", healthStatus);
        System.out.printf("   Total Failures: %d%n", metrics.getTotalFailureCount());
        
        if (metrics.getNextPartitionsToDelete() != null && !metrics.getNextPartitionsToDelete().isEmpty()) {
            System.out.printf("   Next Cleanup: %d partitions%n", metrics.getNextPartitionsToDelete().size());
        }
        System.out.println();
        
        // Delivery Method
        System.out.println("📡 MONITORING CONFIG:");
        System.out.printf("   Delivery: %s%n", config.getDeliveryMethod().name());
        System.out.printf("   Format: %s%n", config.getMetricFormat().name());
        System.out.printf("   Interval: %d seconds%n", config.getReportingIntervalSeconds());
        
        switch (config.getDeliveryMethod()) {
            case HTTP_ENDPOINT:
                System.out.printf("   Endpoint: %s%n", config.getHttpEndpointPath());
                break;
            case REST_PUSH:
                System.out.printf("   Push URL: %s%n", config.getRestPushUrl());
                break;
            case KAFKA_PUSH:
                System.out.printf("   Kafka Topic: %s%n", config.getKafkaTopicName());
                break;
        }
        
        System.out.println("========================================");
        System.out.println();
    }
    
    /**
     * Record initial metrics when monitoring starts
     */
    private void recordInitialMetrics() {
        try {
            // Collect initial partition info
            collectPartitionMetrics();
            
            // Print initial state
            printSimplifiedMetrics();
        } catch (Exception e) {
            System.err.printf("Error recording initial metrics: %s%n", e.getMessage());
        }
    }
}