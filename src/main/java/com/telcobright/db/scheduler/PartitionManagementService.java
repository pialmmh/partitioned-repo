package com.telcobright.db.scheduler;

import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.TableManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central service for managing partition scheduling across all entities
 * Automatically starts schedulers for entities with autoManagePartition=true
 */
public class PartitionManagementService {
    
    private final DataSource dataSource;
    private final ConcurrentHashMap<String, PartitionScheduler> schedulers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TableManager> tableManagers = new ConcurrentHashMap<>();
    
    public PartitionManagementService(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    /**
     * Register an entity for partition management
     */
    public <T> void registerEntity(Class<T> entityClass) {
        EntityMetadata<T> metadata = EntityMetadata.of(entityClass);
        
        if (!metadata.isAutoManagePartition()) {
            System.out.println("Skipping partition management for " + entityClass.getSimpleName() + 
                             " (autoManagePartition=false)");
            return;
        }
        
        String entityKey = entityClass.getSimpleName();
        // Use a placeholder database name - the factory will override this with proper TableManager
        TableManager tableManager = new TableManager(dataSource, metadata, "default_db");
        
        // Note: Table initialization is handled by the factory to ensure proper configuration
        // The scheduler only manages daily maintenance, not initial setup
        
        // Store table manager
        tableManagers.put(entityKey, tableManager);
        
        // Create and start scheduler if not already running for this adjustment time
        String adjustmentTime = metadata.getPartitionAdjustmentTime();
        String schedulerKey = "scheduler_" + adjustmentTime.replace(":", "");
        
        schedulers.computeIfAbsent(schedulerKey, key -> {
            // Collect all table managers with the same adjustment time
            List<TableManager> managersForTime = new ArrayList<>();
            managersForTime.add(tableManager);
            
            // Add any existing managers with same adjustment time
            for (String existingKey : tableManagers.keySet()) {
                if (!existingKey.equals(entityKey)) {
                    try {
                        Class<?> existingClass = Class.forName("com.telcobright.db.example." + existingKey);
                        EntityMetadata<?> existingMetadata = EntityMetadata.of(existingClass);
                        if (existingMetadata.getPartitionAdjustmentTime().equals(adjustmentTime)) {
                            managersForTime.add(tableManagers.get(existingKey));
                        }
                    } catch (ClassNotFoundException e) {
                        // Ignore - class might not exist yet
                    }
                }
            }
            
            PartitionScheduler scheduler = new PartitionScheduler(managersForTime, adjustmentTime);
            scheduler.start();
            return scheduler;
        });
        
        System.out.println("Registered " + entityClass.getSimpleName() + 
                          " for automatic partition management (adjustment time: " + adjustmentTime + ")");
    }
    
    /**
     * Get table manager for an entity
     */
    public TableManager getTableManager(Class<?> entityClass) {
        return tableManagers.get(entityClass.getSimpleName());
    }
    
    /**
     * Manually trigger maintenance for a specific entity
     */
    public void triggerMaintenance(Class<?> entityClass) {
        TableManager tableManager = tableManagers.get(entityClass.getSimpleName());
        if (tableManager != null) {
            try {
                tableManager.performDailyMaintenance();
                System.out.println("Manual maintenance completed for " + entityClass.getSimpleName());
            } catch (Exception e) {
                System.err.println("Failed to perform manual maintenance: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Manually trigger maintenance for all entities
     */
    public void triggerMaintenanceForAll() {
        System.out.println("Triggering manual maintenance for all entities...");
        for (String entityKey : tableManagers.keySet()) {
            try {
                tableManagers.get(entityKey).performDailyMaintenance();
                System.out.println("Manual maintenance completed for " + entityKey);
            } catch (Exception e) {
                System.err.println("Failed to perform manual maintenance for " + entityKey + ": " + e.getMessage());
            }
        }
    }
    
    /**
     * Stop all schedulers
     */
    public void shutdown() {
        System.out.println("Shutting down partition management service...");
        for (PartitionScheduler scheduler : schedulers.values()) {
            scheduler.stop();
        }
        schedulers.clear();
        tableManagers.clear();
        System.out.println("Partition management service stopped");
    }
    
    /**
     * Get status of all registered entities
     */
    public void printStatus() {
        System.out.println("=== Partition Management Status ===");
        System.out.println("Registered entities: " + tableManagers.size());
        System.out.println("Active schedulers: " + schedulers.size());
        
        for (String entityKey : tableManagers.keySet()) {
            try {
                Class<?> entityClass = Class.forName("com.telcobright.db.example." + entityKey);
                EntityMetadata<?> metadata = EntityMetadata.of(entityClass);
                
                System.out.println("• " + entityKey + ":");
                System.out.println("  - Mode: " + metadata.getShardingMode());
                System.out.println("  - Retention: " + metadata.getRetentionSpanDays() + " days");
                System.out.println("  - Adjustment time: " + metadata.getPartitionAdjustmentTime());
                System.out.println("  - Auto-manage: " + metadata.isAutoManagePartition());
            } catch (ClassNotFoundException e) {
                System.out.println("• " + entityKey + ": [metadata not accessible]");
            }
        }
    }
}