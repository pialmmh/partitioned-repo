package com.telcobright.db.monitoring;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interface for repository monitoring service.
 * Provides methods for starting/stopping monitoring and updating metrics.
 */
public interface MonitoringService {
    
    /**
     * Start monitoring with the configured reporting interval
     */
    void start();
    
    /**
     * Stop monitoring
     */
    void stop();
    
    /**
     * Check if monitoring is currently running
     */
    boolean isRunning();
    
    /**
     * Get the current metrics snapshot
     */
    RepositoryMetrics getMetrics();
    
    /**
     * Update metrics after a maintenance job run
     */
    void recordMaintenanceJobStart();
    
    /**
     * Record successful completion of a maintenance job
     */
    void recordMaintenanceJobSuccess(long durationMs, int partitionsCreated, int partitionsDeleted);
    
    /**
     * Record failure of a maintenance job
     */
    void recordMaintenanceJobFailure(long durationMs, Throwable error);
    
    /**
     * Update partition and table counts
     */
    void updatePartitionInfo(int partitionCount, int prefixedTableCount);
    
    /**
     * Get the monitoring configuration
     */
    MonitoringConfig getConfig();
    
    /**
     * Get metrics in Prometheus format (if supported)
     */
    String getPrometheusMetrics();
    
    /**
     * Get metrics in JSON format
     */
    String getJsonMetrics();
}