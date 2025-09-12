package com.telcobright.core.partition;

/**
 * Defines the available partitioning strategies for Split-Verse.
 * 
 * Each strategy determines how data is distributed across partitions
 * within a single shard based on different criteria.
 */
public enum PartitionType {
    
    /**
     * Date-based partitioning using LocalDateTime field.
     * Partitions data by time ranges (daily, monthly, yearly).
     * Currently the only implemented strategy.
     */
    DATE_BASED("Date-based partitioning by LocalDateTime field"),
    
    /**
     * Hash-based partitioning using modulo on numeric field.
     * Would partition data evenly across a fixed number of partitions.
     * NOT YET IMPLEMENTED.
     */
    HASH_BASED("Hash-based partitioning on numeric field"),
    
    /**
     * Range-based partitioning on numeric ID.
     * Would partition by ID ranges (e.g., 1-1000000, 1000001-2000000).
     * NOT YET IMPLEMENTED.
     */
    RANGE_BASED("Range-based partitioning on numeric field"),
    
    /**
     * List-based partitioning on categorical field.
     * Would partition by specific values (e.g., by country, status).
     * NOT YET IMPLEMENTED.
     */
    LIST_BASED("List-based partitioning on categorical values"),
    
    /**
     * Composite partitioning combining multiple strategies.
     * Would support sub-partitioning (e.g., by date then by hash).
     * NOT YET IMPLEMENTED.
     */
    COMPOSITE("Composite partitioning with multiple strategies");
    
    private final String description;
    
    PartitionType(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * Check if this partition type is currently implemented.
     * 
     * @return true if implemented, false otherwise
     */
    public boolean isImplemented() {
        return this == DATE_BASED;
    }
    
    /**
     * Validate that the partition type is supported.
     * 
     * @throws UnsupportedOperationException if not implemented
     */
    public void validateSupported() {
        if (!isImplemented()) {
            throw new UnsupportedOperationException(
                String.format("Partition type %s is not yet implemented. Currently only %s is supported.", 
                    this.name(), DATE_BASED.name())
            );
        }
    }
}