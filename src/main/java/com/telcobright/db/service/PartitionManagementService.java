package com.telcobright.db.service;

import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.sharding.ShardingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class PartitionManagementService {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitionManagementService.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private final ShardingConfig config;
    private final DataSource dataSource;
    
    public PartitionManagementService(ShardingConfig config) {
        this.config = config;
        this.dataSource = config.getDataSource();
    }
    
    public void initializePartitions() {
        logger.info("Initializing partitions for entity: {}", config.getEntityName());
        
        LocalDate today = LocalDate.now();
        LocalDate startDate = today.minusDays(config.getRetentionSpanDays());
        LocalDate endDate = today.plusDays(config.getRetentionSpanDays());
        
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            initializePartitionedTable(startDate, endDate);
        } else if (config.getMode() == ShardingMode.MULTI_TABLE) {
            initializeMultipleTables(startDate, endDate);
        }
        
        logger.info("Partition initialization completed for entity: {}", config.getEntityName());
    }
    
    public void cleanupExpiredPartitions() {
        logger.info("Cleaning up expired partitions for entity: {}", config.getEntityName());
        
        LocalDate cutoffDate = LocalDate.now().minusDays(config.getRetentionSpanDays());
        
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            cleanupExpiredPartitions(cutoffDate);
        } else if (config.getMode() == ShardingMode.MULTI_TABLE) {
            cleanupExpiredTables(cutoffDate);
        }
        
        logger.info("Expired partition cleanup completed for entity: {}", config.getEntityName());
    }
    
    public void createPartitionForDate(LocalDate date) {
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            createPartitionedTablePartition(date);
        } else if (config.getMode() == ShardingMode.MULTI_TABLE) {
            createTable(date);
        }
    }
    
    private void initializePartitionedTable(LocalDate startDate, LocalDate endDate) {
        try (Connection conn = dataSource.getConnection()) {
            createBaseTableIfNotExists(conn);
            
            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                createPartitionedTablePartition(current);
                current = current.plusDays(1);
            }
        } catch (SQLException e) {
            logger.error("Error initializing partitioned table", e);
            throw new RuntimeException("Failed to initialize partitioned table", e);
        }
    }
    
    private void initializeMultipleTables(LocalDate startDate, LocalDate endDate) {
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            createTable(current);
            current = current.plusDays(1);
        }
    }
    
    private void createBaseTableIfNotExists(Connection conn) throws SQLException {
        String tableName = config.getEntityName();
        
        if (!tableExists(conn, tableName)) {
            String createTableSql = getCreateBaseTableSql();
            try (PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
                stmt.executeUpdate();
                logger.info("Created base table: {}", tableName);
            }
        }
    }
    
    private void createPartitionedTablePartition(LocalDate date) {
        String partitionName = config.getEntityName() + "_" + date.format(DATE_FORMAT);
        
        try (Connection conn = dataSource.getConnection()) {
            if (!partitionExists(conn, partitionName)) {
                String createPartitionSql = getCreatePartitionSql(partitionName, date);
                try (PreparedStatement stmt = conn.prepareStatement(createPartitionSql)) {
                    stmt.executeUpdate();
                    logger.info("Created partition: {}", partitionName);
                }
            }
        } catch (SQLException e) {
            logger.error("Error creating partition: {}", partitionName, e);
            throw new RuntimeException("Failed to create partition: " + partitionName, e);
        }
    }
    
    private void createTable(LocalDate date) {
        String tableName = config.getEntityName() + date.format(DATE_FORMAT);
        
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                String createTableSql = getCreateTableSql(tableName);
                try (PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
                    stmt.executeUpdate();
                    logger.info("Created table: {}", tableName);
                }
            }
        } catch (SQLException e) {
            logger.error("Error creating table: {}", tableName, e);
            throw new RuntimeException("Failed to create table: " + tableName, e);
        }
    }
    
    private void cleanupExpiredPartitions(LocalDate cutoffDate) {
        try (Connection conn = dataSource.getConnection()) {
            List<String> expiredPartitions = findExpiredPartitions(conn, cutoffDate);
            
            for (String partitionName : expiredPartitions) {
                dropPartition(conn, partitionName);
                logger.info("Dropped expired partition: {}", partitionName);
            }
        } catch (SQLException e) {
            logger.error("Error cleaning up expired partitions", e);
            throw new RuntimeException("Failed to cleanup expired partitions", e);
        }
    }
    
    private void cleanupExpiredTables(LocalDate cutoffDate) {
        try (Connection conn = dataSource.getConnection()) {
            List<String> expiredTables = findExpiredTables(conn, cutoffDate);
            
            for (String tableName : expiredTables) {
                dropTable(conn, tableName);
                logger.info("Dropped expired table: {}", tableName);
            }
        } catch (SQLException e) {
            logger.error("Error cleaning up expired tables", e);
            throw new RuntimeException("Failed to cleanup expired tables", e);
        }
    }
    
    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    private boolean partitionExists(Connection conn, String partitionName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM information_schema.partitions WHERE partition_name = ? AND table_name = ? AND table_schema = DATABASE()";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, partitionName);
            stmt.setString(2, config.getEntityName());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    private List<String> findExpiredPartitions(Connection conn, LocalDate cutoffDate) throws SQLException {
        List<String> expiredPartitions = new ArrayList<>();
        String entityPrefix = config.getEntityName() + "_";
        
        String sql = "SELECT partition_name FROM information_schema.partitions WHERE table_name = ? AND table_schema = DATABASE() AND partition_name IS NOT NULL";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, config.getEntityName());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String partitionName = rs.getString("partition_name");
                    if (partitionName.startsWith(entityPrefix)) {
                        String dateStr = partitionName.substring(entityPrefix.length());
                        try {
                            LocalDate partitionDate = LocalDate.parse(dateStr, DATE_FORMAT);
                            if (partitionDate.isBefore(cutoffDate)) {
                                expiredPartitions.add(partitionName);
                            }
                        } catch (Exception e) {
                            logger.warn("Could not parse date from partition name: {}", partitionName);
                        }
                    }
                }
            }
        }
        
        return expiredPartitions;
    }
    
    private List<String> findExpiredTables(Connection conn, LocalDate cutoffDate) throws SQLException {
        List<String> expiredTables = new ArrayList<>();
        String entityPrefix = config.getEntityName();
        
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, entityPrefix + "%");
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    if (tableName.startsWith(entityPrefix) && tableName.length() > entityPrefix.length()) {
                        String dateStr = tableName.substring(entityPrefix.length());
                        try {
                            LocalDate tableDate = LocalDate.parse(dateStr, DATE_FORMAT);
                            if (tableDate.isBefore(cutoffDate)) {
                                expiredTables.add(tableName);
                            }
                        } catch (Exception e) {
                            logger.warn("Could not parse date from table name: {}", tableName);
                        }
                    }
                }
            }
        }
        
        return expiredTables;
    }
    
    private void dropPartition(Connection conn, String partitionName) throws SQLException {
        String sql = String.format("ALTER TABLE %s DROP PARTITION %s", config.getEntityName(), partitionName);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }
    
    private void dropTable(Connection conn, String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }
    
    private String getCreateBaseTableSql() {
        return String.format("CREATE TABLE IF NOT EXISTS %s (%s) PARTITION BY RANGE(TO_DAYS(%s)) ()", 
                           config.getEntityName(), getTableColumns(), config.getShardKey());
    }
    
    private String getCreatePartitionSql(String partitionName, LocalDate date) {
        LocalDate nextDate = date.plusDays(1);
        return String.format("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (TO_DAYS('%s')))",
                           config.getEntityName(), partitionName, nextDate.toString());
    }
    
    private String getCreateTableSql(String tableName) {
        return String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, getTableColumns());
    }
    
    protected String getTableColumns() {
        return "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
               config.getShardKey() + " DATETIME NOT NULL, " +
               "data JSON, " +
               "INDEX idx_shard_key (" + config.getShardKey() + ")";
    }
}