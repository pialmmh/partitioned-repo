package com.telcobright.db.scheduler;

import com.telcobright.db.repository.TableManager;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler for automatic partition/table management
 * Runs daily maintenance at configured partitionAdjustmentTime
 */
public class PartitionScheduler {
    
    private final ScheduledExecutorService scheduler;
    private final List<TableManager> tableManagers;
    private final String adjustmentTime; // Format: "HH:mm" (e.g., "04:00")
    
    public PartitionScheduler(List<TableManager> tableManagers, String adjustmentTime) {
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.tableManagers = tableManagers;
        this.adjustmentTime = adjustmentTime;
    }
    
    /**
     * Start the scheduler - calculates initial delay to next adjustment time
     */
    public void start() {
        long initialDelay = calculateInitialDelay();
        long period = TimeUnit.DAYS.toSeconds(1); // Run daily
        
        scheduler.scheduleAtFixedRate(
            this::performDailyMaintenance,
            initialDelay,
            period,
            TimeUnit.SECONDS
        );
        
        System.out.println("Partition scheduler started - next maintenance at " + getNextMaintenanceTime());
    }
    
    /**
     * Stop the scheduler
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Partition scheduler stopped");
    }
    
    /**
     * Perform daily maintenance on all registered table managers
     */
    private void performDailyMaintenance() {
        System.out.println("Starting daily partition maintenance at " + LocalDateTime.now());
        
        for (TableManager tableManager : tableManagers) {
            try {
                tableManager.performDailyMaintenance();
            } catch (Exception e) {
                System.err.println("Failed to perform maintenance on table manager: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("Daily partition maintenance completed");
    }
    
    /**
     * Calculate seconds until next adjustment time
     */
    private long calculateInitialDelay() {
        LocalTime targetTime = LocalTime.parse(adjustmentTime, DateTimeFormatter.ofPattern("HH:mm"));
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun = now.toLocalDate().atTime(targetTime);
        
        // If target time has already passed today, schedule for tomorrow
        if (nextRun.isBefore(now) || nextRun.isEqual(now)) {
            nextRun = nextRun.plusDays(1);
        }
        
        return java.time.Duration.between(now, nextRun).getSeconds();
    }
    
    /**
     * Get next maintenance time as string
     */
    private String getNextMaintenanceTime() {
        LocalTime targetTime = LocalTime.parse(adjustmentTime, DateTimeFormatter.ofPattern("HH:mm"));
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun = now.toLocalDate().atTime(targetTime);
        
        if (nextRun.isBefore(now) || nextRun.isEqual(now)) {
            nextRun = nextRun.plusDays(1);
        }
        
        return nextRun.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
    }
    
    /**
     * Manually trigger maintenance (for testing)
     */
    public void triggerMaintenance() {
        performDailyMaintenance();
    }
}