package com.telcobright.core.partition;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.annotation.ShardingKey;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;

/**
 * Date-based partitioning strategy implementation.
 * Partitions data by LocalDateTime field using MySQL RANGE partitioning.
 * Creates daily partitions by default.
 */
public class DateBasedPartitionStrategy<T extends ShardingEntity> implements PartitionStrategy<T> {
    
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final String partitionKeyColumn;
    private final PartitionGranularity granularity;
    
    /**
     * Granularity of date-based partitions.
     */
    public enum PartitionGranularity {
        DAILY("Daily partitions"),
        MONTHLY("Monthly partitions"),
        YEARLY("Yearly partitions");
        
        private final String description;
        
        PartitionGranularity(String description) {
            this.description = description;
        }
    }
    
    /**
     * Create date-based partition strategy with default daily granularity.
     * 
     * @param partitionKeyColumn Column name to partition by (must be LocalDateTime)
     */
    public DateBasedPartitionStrategy(String partitionKeyColumn) {
        this(partitionKeyColumn, PartitionGranularity.DAILY);
    }
    
    /**
     * Create date-based partition strategy with specified granularity.
     * 
     * @param partitionKeyColumn Column name to partition by
     * @param granularity Partition granularity (daily, monthly, yearly)
     */
    public DateBasedPartitionStrategy(String partitionKeyColumn, PartitionGranularity granularity) {
        this.partitionKeyColumn = partitionKeyColumn;
        this.granularity = granularity;
    }
    
    @Override
    public PartitionType getType() {
        return PartitionType.DATE_BASED;
    }
    
    @Override
    public String generatePartitionByClause(String partitionKeyColumn) {
        return String.format("PARTITION BY RANGE (TO_DAYS(%s))", partitionKeyColumn);
    }
    
    @Override
    public String generateInitialPartitions(Object startValue, Object endValue) {
        if (!(startValue instanceof LocalDateTime) || !(endValue instanceof LocalDateTime)) {
            throw new IllegalArgumentException("Date-based partitioning requires LocalDateTime values");
        }
        
        LocalDateTime start = (LocalDateTime) startValue;
        LocalDateTime end = (LocalDateTime) endValue;
        
        StringBuilder partitions = new StringBuilder(" (\n");
        LocalDateTime current = start;
        boolean first = true;
        
        while (!current.isAfter(end)) {
            if (!first) {
                partitions.append(",\n");
            }
            
            String partitionName = generatePartitionName(current);
            LocalDateTime nextDay = current.plusDays(1);
            String partitionValue = String.format("TO_DAYS('%s')", nextDay.toLocalDate());
            
            partitions.append(String.format("  PARTITION %s VALUES LESS THAN (%s)", 
                partitionName, partitionValue));
            
            first = false;
            current = getNextPartitionDate(current);
        }
        
        partitions.append("\n)");
        return partitions.toString();
    }
    
    @Override
    public void createPartition(Connection connection, String tableName, 
                               String partitionName, Object partitionValue) throws SQLException {
        if (!(partitionValue instanceof LocalDateTime)) {
            throw new IllegalArgumentException("Date-based partitioning requires LocalDateTime value");
        }
        
        LocalDateTime date = (LocalDateTime) partitionValue;
        LocalDateTime nextDate = getNextPartitionDate(date);
        String sqlValue = String.format("TO_DAYS('%s')", nextDate.toLocalDate());
        
        String sql = String.format("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s))",
            tableName, partitionName, sqlValue);
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }
    
    @Override
    public void dropPartition(Connection connection, String tableName, 
                             String partitionName) throws SQLException {
        String sql = String.format("ALTER TABLE %s DROP PARTITION %s", tableName, partitionName);
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }
    
    @Override
    public List<String> getPartitions(Connection connection, String database, 
                                     String tableName) throws SQLException {
        List<String> partitions = new ArrayList<>();
        
        String sql = "SELECT partition_name FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? " +
                    "AND partition_name IS NOT NULL " +
                    "ORDER BY partition_ordinal_position";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getString("partition_name"));
                }
            }
        }
        
        return partitions;
    }
    
    @Override
    public boolean partitionExists(Connection connection, String database, 
                                  String tableName, String partitionName) throws SQLException {
        String sql = "SELECT 1 FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? AND partition_name = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            stmt.setString(3, partitionName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    
    @Override
    public String generatePartitionName(Object value) {
        if (!(value instanceof LocalDateTime)) {
            throw new IllegalArgumentException("Date-based partitioning requires LocalDateTime value");
        }
        
        LocalDateTime date = (LocalDateTime) value;
        
        switch (granularity) {
            case DAILY:
                return "p" + date.format(DATE_FORMAT);
            case MONTHLY:
                return "p" + date.format(DateTimeFormatter.ofPattern("yyyyMM"));
            case YEARLY:
                return "p" + date.format(DateTimeFormatter.ofPattern("yyyy"));
            default:
                return "p" + date.format(DATE_FORMAT);
        }
    }
    
    @Override
    public String getPartitionKeyColumn() {
        return partitionKeyColumn;
    }
    
    @Override
    public void validateEntity(Class<T> entityClass) {
        // Check that entity has a LocalDateTime field with @ShardingKey
        boolean hasValidShardingKey = false;
        
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(ShardingKey.class)) {
                if (!LocalDateTime.class.isAssignableFrom(field.getType())) {
                    throw new IllegalArgumentException(
                        String.format("Date-based partitioning requires @ShardingKey field to be LocalDateTime, " +
                                     "but found %s in entity %s", 
                                     field.getType().getSimpleName(), 
                                     entityClass.getSimpleName())
                    );
                }
                hasValidShardingKey = true;
                break;
            }
        }
        
        if (!hasValidShardingKey) {
            throw new IllegalArgumentException(
                String.format("Date-based partitioning requires entity %s to have a LocalDateTime field " +
                             "annotated with @ShardingKey", entityClass.getSimpleName())
            );
        }
    }
    
    @Override
    public List<PartitionDefinition> getPartitionsForRetentionPeriod(int retentionDays) {
        List<PartitionDefinition> partitions = new ArrayList<>();
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(retentionDays);
        LocalDateTime endDate = now.plusDays(retentionDays);
        
        LocalDateTime current = startDate;
        while (!current.isAfter(endDate)) {
            String partitionName = generatePartitionName(current);
            LocalDateTime nextDate = getNextPartitionDate(current);
            
            partitions.add(new PartitionDefinition(partitionName, nextDate));
            current = nextDate;
        }
        
        return partitions;
    }
    
    @Override
    public List<String> getPartitionsToDropp(Connection connection, String database, 
                                            String tableName, Object cutoffValue) throws SQLException {
        if (!(cutoffValue instanceof LocalDateTime)) {
            throw new IllegalArgumentException("Date-based partitioning requires LocalDateTime cutoff value");
        }
        
        LocalDateTime cutoff = (LocalDateTime) cutoffValue;
        String cutoffPartitionName = generatePartitionName(cutoff);
        
        List<String> allPartitions = getPartitions(connection, database, tableName);
        List<String> partitionsToDrop = new ArrayList<>();
        
        for (String partition : allPartitions) {
            // Compare partition names (they're date-based and sortable)
            if (partition.compareTo(cutoffPartitionName) < 0) {
                partitionsToDrop.add(partition);
            }
        }
        
        return partitionsToDrop;
    }
    
    /**
     * Get the next partition date based on granularity.
     */
    private LocalDateTime getNextPartitionDate(LocalDateTime current) {
        switch (granularity) {
            case DAILY:
                return current.plusDays(1);
            case MONTHLY:
                return current.plusMonths(1).withDayOfMonth(1);
            case YEARLY:
                return current.plusYears(1).withDayOfYear(1);
            default:
                return current.plusDays(1);
        }
    }
}