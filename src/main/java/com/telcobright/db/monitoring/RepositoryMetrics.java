package com.telcobright.db.monitoring;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Container for repository metrics as specified in CLAUDE-MONITORING.md
 */
public class RepositoryMetrics {
    
    // Scheduler metrics
    private volatile LocalDateTime lastRunTimestamp;
    private volatile LocalDateTime nextScheduledTime;
    private volatile List<String> nextPartitionsToDelete;
    private volatile long lastJobDurationMs;
    
    // Partition maintenance metrics
    private final String repositoryType; // "Partitioned" or "MultiTable"
    private final String repositoryName; // Prefix like "sms"
    private volatile int numberOfPartitions;
    private volatile int numberOfPrefixedTables;
    private volatile List<String> totalListOfPartitions;
    private volatile int partitionsCreatedLastRun;
    private volatile int partitionsDeletedLastRun;
    
    // Error metrics
    private volatile boolean lastMaintenanceJobSucceeded;
    private final AtomicLong totalFailureCount = new AtomicLong(0);
    
    // Service info
    private final String hostname;
    private final LocalDateTime startupTimestamp;
    
    public RepositoryMetrics(String repositoryType, String repositoryName, String hostname) {
        this.repositoryType = repositoryType;
        this.repositoryName = repositoryName;
        this.hostname = hostname;
        this.startupTimestamp = LocalDateTime.now();
        this.lastMaintenanceJobSucceeded = true; // Default to success
    }
    
    // Scheduler metrics setters/getters
    public void setLastRunTimestamp(LocalDateTime timestamp) {
        this.lastRunTimestamp = timestamp;
    }
    
    public LocalDateTime getLastRunTimestamp() {
        return lastRunTimestamp;
    }
    
    public void setNextScheduledTime(LocalDateTime time) {
        this.nextScheduledTime = time;
    }
    
    public LocalDateTime getNextScheduledTime() {
        return nextScheduledTime;
    }
    
    public void setNextPartitionsToDelete(List<String> partitions) {
        this.nextPartitionsToDelete = partitions;
    }
    
    public List<String> getNextPartitionsToDelete() {
        return nextPartitionsToDelete;
    }
    
    public void setLastJobDurationMs(long durationMs) {
        this.lastJobDurationMs = durationMs;
    }
    
    public long getLastJobDurationMs() {
        return lastJobDurationMs;
    }
    
    // Partition maintenance metrics setters/getters
    public String getRepositoryType() {
        return repositoryType;
    }
    
    public String getRepositoryName() {
        return repositoryName;
    }
    
    public void setNumberOfPartitions(int count) {
        this.numberOfPartitions = count;
    }
    
    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }
    
    public void setNumberOfPrefixedTables(int count) {
        this.numberOfPrefixedTables = count;
    }
    
    public int getNumberOfPrefixedTables() {
        return numberOfPrefixedTables;
    }
    
    public void setTotalListOfPartitions(List<String> partitions) {
        this.totalListOfPartitions = partitions;
    }
    
    public List<String> getTotalListOfPartitions() {
        return totalListOfPartitions;
    }
    
    public void setPartitionsCreatedLastRun(int count) {
        this.partitionsCreatedLastRun = count;
    }
    
    public int getPartitionsCreatedLastRun() {
        return partitionsCreatedLastRun;
    }
    
    public void setPartitionsDeletedLastRun(int count) {
        this.partitionsDeletedLastRun = count;
    }
    
    public int getPartitionsDeletedLastRun() {
        return partitionsDeletedLastRun;
    }
    
    // Error metrics setters/getters
    public void setLastMaintenanceJobSucceeded(boolean succeeded) {
        this.lastMaintenanceJobSucceeded = succeeded;
        if (!succeeded) {
            totalFailureCount.incrementAndGet();
        }
    }
    
    public boolean isLastMaintenanceJobSucceeded() {
        return lastMaintenanceJobSucceeded;
    }
    
    public long getTotalFailureCount() {
        return totalFailureCount.get();
    }
    
    // Service info getters
    public String getHostname() {
        return hostname;
    }
    
    public LocalDateTime getStartupTimestamp() {
        return startupTimestamp;
    }
    
    // Utility methods for epoch timestamps
    public long getLastRunTimestampEpoch() {
        return lastRunTimestamp != null ? 
            lastRunTimestamp.atZone(java.time.ZoneId.of("UTC")).toEpochSecond() : 0;
    }
    
    public long getNextScheduledTimeEpoch() {
        return nextScheduledTime != null ? 
            nextScheduledTime.atZone(java.time.ZoneId.of("UTC")).toEpochSecond() : 0;
    }
    
    public long getStartupTimestampEpoch() {
        return startupTimestamp.atZone(java.time.ZoneId.of("UTC")).toEpochSecond();
    }
    
    @Override
    public String toString() {
        return String.format(
            "RepositoryMetrics{type=%s, name=%s, partitions=%d, tables=%d, lastSuccess=%s, failures=%d}",
            repositoryType, repositoryName, numberOfPartitions, numberOfPrefixedTables,
            lastMaintenanceJobSucceeded, totalFailureCount.get()
        );
    }
}