package com.telcobright.db.service;

import com.telcobright.db.sharding.ShardingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionSchedulerService {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitionSchedulerService.class);
    
    private final ShardingConfig config;
    private final PartitionManagementService partitionService;
    private final ScheduledExecutorService scheduler;
    private boolean started = false;
    
    public PartitionSchedulerService(ShardingConfig config) {
        this.config = config;
        this.partitionService = new PartitionManagementService(config);
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setName("partition-scheduler-" + config.getEntityName());
            t.setDaemon(true);
            return t;
        });
    }
    
    public synchronized void start() {
        if (started) {
            logger.warn("Partition scheduler for {} already started", config.getEntityName());
            return;
        }
        
        if (!config.isAutoManagePartition()) {
            logger.info("Auto partition management disabled for {}", config.getEntityName());
            return;
        }
        
        long initialDelay = calculateInitialDelay();
        long period = TimeUnit.DAYS.toSeconds(1);
        
        scheduler.scheduleAtFixedRate(
            this::performMaintenanceTasks,
            initialDelay,
            period,
            TimeUnit.SECONDS
        );
        
        started = true;
        logger.info("Partition scheduler started for {} with initial delay {} seconds", 
                   config.getEntityName(), initialDelay);
    }
    
    public synchronized void stop() {
        if (!started) {
            return;
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        started = false;
        logger.info("Partition scheduler stopped for {}", config.getEntityName());
    }
    
    public boolean isStarted() {
        return started;
    }
    
    private void performMaintenanceTasks() {
        try {
            logger.info("Starting partition maintenance for {}", config.getEntityName());
            
            partitionService.cleanupExpiredPartitions();
            
            partitionService.createPartitionForDate(LocalDateTime.now().toLocalDate().plusDays(1));
            
            logger.info("Completed partition maintenance for {}", config.getEntityName());
            
        } catch (Exception e) {
            logger.error("Error during partition maintenance for {}", config.getEntityName(), e);
        }
    }
    
    private long calculateInitialDelay() {
        LocalTime now = LocalTime.now();
        LocalTime targetTime = config.getPartitionAdjustmentTime();
        
        Duration delay;
        if (now.isBefore(targetTime)) {
            delay = Duration.between(now, targetTime);
        } else {
            delay = Duration.between(now, targetTime.plusHours(24));
        }
        
        return delay.getSeconds();
    }
    
    public void triggerManualMaintenance() {
        logger.info("Triggering manual partition maintenance for {}", config.getEntityName());
        scheduler.execute(this::performMaintenanceTasks);
    }
}