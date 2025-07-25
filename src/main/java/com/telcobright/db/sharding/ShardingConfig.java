package com.telcobright.db.sharding;

import javax.sql.DataSource;
import java.time.LocalTime;

public class ShardingConfig {
    private final ShardingMode mode;
    private final String entityName;
    private final String shardKey;
    private final int retentionSpanDays;
    private final LocalTime partitionAdjustmentTime;
    private final String shardingAlgorithmClass;
    private final DataSource dataSource;
    private final boolean autoManagePartition;

    private ShardingConfig(Builder builder) {
        this.mode = builder.mode;
        this.entityName = builder.entityName;
        this.shardKey = builder.shardKey;
        this.retentionSpanDays = builder.retentionSpanDays;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.shardingAlgorithmClass = builder.shardingAlgorithmClass;
        this.dataSource = builder.dataSource;
        this.autoManagePartition = builder.autoManagePartition;
    }

    public ShardingMode getMode() {
        return mode;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getShardKey() {
        return shardKey;
    }

    public int getRetentionSpanDays() {
        return retentionSpanDays;
    }

    public LocalTime getPartitionAdjustmentTime() {
        return partitionAdjustmentTime;
    }

    public String getShardingAlgorithmClass() {
        return shardingAlgorithmClass;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isAutoManagePartition() {
        return autoManagePartition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ShardingMode mode = ShardingMode.PARTITIONED_TABLE;
        private String entityName;
        private String shardKey = "created_at";
        private int retentionSpanDays = 7;
        private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0);
        private String shardingAlgorithmClass;
        private DataSource dataSource;
        private boolean autoManagePartition = true;

        public Builder mode(ShardingMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder shardKey(String shardKey) {
            this.shardKey = shardKey;
            return this;
        }

        public Builder retentionSpanDays(int retentionSpanDays) {
            this.retentionSpanDays = retentionSpanDays;
            return this;
        }

        public Builder partitionAdjustmentTime(LocalTime partitionAdjustmentTime) {
            this.partitionAdjustmentTime = partitionAdjustmentTime;
            return this;
        }

        public Builder shardingAlgorithmClass(String shardingAlgorithmClass) {
            this.shardingAlgorithmClass = shardingAlgorithmClass;
            return this;
        }

        public Builder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder autoManagePartition(boolean autoManagePartition) {
            this.autoManagePartition = autoManagePartition;
            return this;
        }

        public ShardingConfig build() {
            if (entityName == null || entityName.trim().isEmpty()) {
                throw new IllegalArgumentException("Entity name is required");
            }
            if (dataSource == null) {
                throw new IllegalArgumentException("DataSource is required");
            }
            return new ShardingConfig(this);
        }
    }
}