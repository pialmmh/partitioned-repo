package com.telcobright.core.partition;

import com.telcobright.core.entity.ShardingEntity;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Interface for different partitioning strategies.
 * 
 * Implementations define how data is partitioned within a single shard/table.
 * This is different from sharding (distributing across databases) - this is
 * about partitioning within a single database table.
 * 
 * @param <T> Entity type that implements ShardingEntity
 */
public interface PartitionStrategy<T extends ShardingEntity> {
    
    /**
     * Get the partition type this strategy implements.
     */
    PartitionType getType();
    
    /**
     * Generate the PARTITION BY clause for CREATE TABLE statement.
     * 
     * @param partitionKeyColumn The column to partition by
     * @return SQL partition clause (e.g., "PARTITION BY RANGE (TO_DAYS(created_at))")
     */
    String generatePartitionByClause(String partitionKeyColumn);
    
    /**
     * Generate initial partition definitions for CREATE TABLE.
     * 
     * @param startValue Starting value for partitions
     * @param endValue Ending value for partitions
     * @return SQL partition definitions
     */
    String generateInitialPartitions(Object startValue, Object endValue);
    
    /**
     * Create a new partition.
     * 
     * @param connection Database connection
     * @param tableName Full table name (database.table)
     * @param partitionName Name of the partition to create
     * @param partitionValue Value/boundary for the partition
     * @throws SQLException if partition creation fails
     */
    void createPartition(Connection connection, String tableName, 
                        String partitionName, Object partitionValue) throws SQLException;
    
    /**
     * Drop an old partition.
     * 
     * @param connection Database connection
     * @param tableName Full table name (database.table)
     * @param partitionName Name of the partition to drop
     * @throws SQLException if partition drop fails
     */
    void dropPartition(Connection connection, String tableName, 
                      String partitionName) throws SQLException;
    
    /**
     * Get list of existing partitions.
     * 
     * @param connection Database connection
     * @param database Database name
     * @param tableName Table name (without database prefix)
     * @return List of partition names
     * @throws SQLException if query fails
     */
    List<String> getPartitions(Connection connection, String database, 
                               String tableName) throws SQLException;
    
    /**
     * Check if a partition exists.
     * 
     * @param connection Database connection
     * @param database Database name
     * @param tableName Table name
     * @param partitionName Partition name to check
     * @return true if partition exists
     * @throws SQLException if query fails
     */
    boolean partitionExists(Connection connection, String database, 
                           String tableName, String partitionName) throws SQLException;
    
    /**
     * Generate partition name based on value.
     * 
     * @param value Value to generate partition name from
     * @return Partition name (e.g., "p20250912" for date-based)
     */
    String generatePartitionName(Object value);
    
    /**
     * Get the partition key column name for this strategy.
     * 
     * @return Column name used for partitioning
     */
    String getPartitionKeyColumn();
    
    /**
     * Validate that the entity is compatible with this partition strategy.
     * 
     * @param entityClass Entity class to validate
     * @throws IllegalArgumentException if entity is not compatible
     */
    void validateEntity(Class<T> entityClass);
    
    /**
     * Get partitions that should be created for a retention period.
     * 
     * @param retentionDays Number of days to retain data
     * @return List of partition definitions
     */
    List<PartitionDefinition> getPartitionsForRetentionPeriod(int retentionDays);
    
    /**
     * Get partitions that are older than the cutoff and should be dropped.
     * 
     * @param connection Database connection
     * @param database Database name
     * @param tableName Table name
     * @param cutoffValue Cutoff value (e.g., LocalDateTime for date-based)
     * @return List of partition names to drop
     * @throws SQLException if query fails
     */
    List<String> getPartitionsToDropp(Connection connection, String database, 
                                     String tableName, Object cutoffValue) throws SQLException;
    
    /**
     * Inner class to define a partition.
     */
    class PartitionDefinition {
        private final String name;
        private final Object lessThanValue;
        
        public PartitionDefinition(String name, Object lessThanValue) {
            this.name = name;
            this.lessThanValue = lessThanValue;
        }
        
        public String getName() { return name; }
        public Object getLessThanValue() { return lessThanValue; }
    }
}