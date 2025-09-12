package com.telcobright.core.partition;

import com.telcobright.core.entity.ShardingEntity;

/**
 * Factory for creating partition strategies based on type.
 */
public class PartitionStrategyFactory {
    
    /**
     * Create a partition strategy based on the specified type.
     * 
     * @param type Partition type
     * @param partitionKeyColumn Column to partition by
     * @param <T> Entity type
     * @return Partition strategy implementation
     * @throws UnsupportedOperationException if partition type is not implemented
     */
    public static <T extends ShardingEntity> PartitionStrategy<T> createStrategy(
            PartitionType type, String partitionKeyColumn) {
        
        // Validate that the partition type is supported
        type.validateSupported();
        
        switch (type) {
            case DATE_BASED:
                return new DateBasedPartitionStrategy<>(partitionKeyColumn);
                
            case HASH_BASED:
                throw new UnsupportedOperationException(
                    "HASH_BASED partitioning is not yet implemented. " +
                    "This would partition data using hash(column) % N partitions."
                );
                
            case RANGE_BASED:
                throw new UnsupportedOperationException(
                    "RANGE_BASED partitioning is not yet implemented. " +
                    "This would partition data by numeric ID ranges."
                );
                
            case LIST_BASED:
                throw new UnsupportedOperationException(
                    "LIST_BASED partitioning is not yet implemented. " +
                    "This would partition data by specific categorical values."
                );
                
            case COMPOSITE:
                throw new UnsupportedOperationException(
                    "COMPOSITE partitioning is not yet implemented. " +
                    "This would support multi-level partitioning strategies."
                );
                
            default:
                throw new IllegalArgumentException("Unknown partition type: " + type);
        }
    }
    
    /**
     * Create a date-based partition strategy with custom granularity.
     * 
     * @param partitionKeyColumn Column to partition by
     * @param granularity Partition granularity (daily, monthly, yearly)
     * @param <T> Entity type
     * @return Date-based partition strategy
     */
    public static <T extends ShardingEntity> PartitionStrategy<T> createDateBasedStrategy(
            String partitionKeyColumn, 
            DateBasedPartitionStrategy.PartitionGranularity granularity) {
        
        return new DateBasedPartitionStrategy<>(partitionKeyColumn, granularity);
    }
    
    /**
     * Get the default partition strategy (date-based with daily granularity).
     * 
     * @param partitionKeyColumn Column to partition by
     * @param <T> Entity type
     * @return Default partition strategy
     */
    public static <T extends ShardingEntity> PartitionStrategy<T> getDefaultStrategy(
            String partitionKeyColumn) {
        
        return new DateBasedPartitionStrategy<>(partitionKeyColumn);
    }
}