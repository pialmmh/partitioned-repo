package com.telcobright.splitverse.tests;

import com.telcobright.core.config.DataSourceConfig;
import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.enums.ShardingStrategy;
import com.telcobright.core.repository.SplitVerseRepository;
import com.telcobright.splitverse.config.RepositoryMode;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for API validation without database connectivity.
 */
public class ApiValidationTest {

    public static class TestEntity implements ShardingEntity<LocalDateTime> {
        private String id;
        private LocalDateTime createdAt;

        @Override
        public String getId() { return id; }
        @Override
        public void setId(String id) { this.id = id; }
        @Override
        public LocalDateTime getCreatedAt() { return createdAt; }
        @Override
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    }

    @Test
    public void testRequiredFieldValidation() {
        // Test missing entity class
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail when entity class is missing");

        // Test missing data sources
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .build();
        }, "Should fail when data sources are missing");

        // Test empty data sources
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withDataSources(Arrays.asList())
                .build();
        }, "Should fail with empty data sources");
    }

    @Test
    public void testDualKeyStrategyValidation() {
        // Test missing partition column for DUAL_KEY_HASH_RANGE
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_RANGE)
                .withPartitionRange(PartitionRange.DAILY)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail when partition column is missing for DUAL_KEY strategy");

        // Test missing partition range for DUAL_KEY_HASH_HASH
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.DUAL_KEY_HASH_HASH)
                .withPartitionColumn("customer_id", PartitionColumnType.STRING)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail when partition range is missing for DUAL_KEY strategy");
    }

    @Test
    public void testIdSizeValidation() {
        // Test ID size too small
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withIdSize(7) // Less than minimum 8
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail when ID size is less than 8");

        // Test ID size too large
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withIdSize(23) // More than maximum 22
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail when ID size is greater than 22");
    }

    @Test
    public void testDataSourceConfigValidation() {
        // Test null host
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create(null, 3306, "test");
        }, "Should fail with null host");

        // Test empty host
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("", 3306, "test");
        }, "Should fail with empty host");

        // Test invalid port (0)
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("localhost", 0, "test");
        }, "Should fail with port 0");

        // Test invalid port (negative)
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("localhost", -1, "test");
        }, "Should fail with negative port");

        // Test invalid port (too large)
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("localhost", 70000, "test");
        }, "Should fail with port > 65535");

        // Test null database
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("localhost", 3306, null);
        }, "Should fail with null database");

        // Test empty database
        assertThrows(IllegalArgumentException.class, () -> {
            DataSourceConfig.create("localhost", 3306, "");
        }, "Should fail with empty database");
    }

    @Test
    public void testRetentionDaysValidation() {
        // Test invalid retention days (0)
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withRetentionDays(0)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail with retention days 0");

        // Test invalid retention days (negative)
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("test")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withRetentionDays(-1)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail with negative retention days");
    }

    @Test
    public void testTableNameValidation() {
        // Test null table name
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName(null)
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail with null table name");

        // Test empty table name
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail with empty table name");

        // Test whitespace-only table name
        assertThrows(IllegalArgumentException.class, () -> {
            SplitVerseRepository.<TestEntity, LocalDateTime>builder()
                .withEntityClass(TestEntity.class)
                .withTableName("   ")
                .withShardingStrategy(ShardingStrategy.SINGLE_KEY_HASH)
                .withDataSources(Arrays.asList(
                    DataSourceConfig.create("localhost", 3306, "test")
                ))
                .build();
        }, "Should fail with whitespace-only table name");
    }
}