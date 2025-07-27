package com.telcobright.db;

import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.mode.repository.standalone.StandalonePersistRepositoryConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.ComplexShardingStrategyConfiguration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Factory for creating Apache ShardingSphere DataSource with proper API usage
 * Framework-agnostic - accepts DataSource through constructor
 */
public class ShardingSphereFactory {
    
    /**
     * Creates a ShardingSphere DataSource with proper sharding configuration
     * 
     * @param actualDataSource The actual MySQL DataSource (e.g., HikariCP)
     * @param entityName The entity/table name (e.g., "sms")
     * @param shardKey The sharding column (e.g., "created_at")
     * @param retentionDays Number of days to retain data
     * @return ShardingSphere DataSource with sharding rules configured
     */
    public static DataSource createShardingSphereDataSource(
            DataSource actualDataSource,
            String entityName,
            String shardKey,
            int retentionDays) throws SQLException {
        
        try {
            // Create data source map
            Map<String, DataSource> dataSourceMap = new HashMap<>();
            dataSourceMap.put("ds0", actualDataSource);
            
            // Create sharding rule configuration
            ShardingRuleConfiguration shardingRuleConfig = createShardingRuleConfig(
                entityName, shardKey, retentionDays
            );
            
            // Create mode configuration (standalone mode without persistence)
            // Using null for no persistence - ShardingSphere will use in-memory metadata
            ModeConfiguration modeConfig = new ModeConfiguration("Standalone", null);
            
            // Properties for ShardingSphere
            Properties props = new Properties();
            props.setProperty("sql-show", "true"); // Show SQL in logs
            
            // Create ShardingSphere DataSource (correct parameter order for 5.4.1)
            DataSource shardingSphereDataSource = ShardingSphereDataSourceFactory.createDataSource(
                modeConfig,
                dataSourceMap,
                Collections.singleton(shardingRuleConfig),
                props
            );
            
            System.out.println("✓ ShardingSphere DataSource created with proper API:");
            System.out.println("  Entity: " + entityName);
            System.out.println("  Shard Key: " + shardKey);
            System.out.println("  Retention Days: " + retentionDays);
            System.out.println("  Table Sharding: Date-based multi-table sharding (1 table/day)");
            System.out.println("  Mode: Programmatic configuration with ShardingSphere 5.4.1");
            
            return shardingSphereDataSource;
            
        } catch (Exception e) {
            System.err.println("Failed to create ShardingSphere DataSource: " + e.getMessage());
            System.err.println("Falling back to original DataSource with manual routing");
            return actualDataSource;
        }
    }
    
    /**
     * Create sharding rule configuration for multi-table date-based sharding
     */
    private static ShardingRuleConfiguration createShardingRuleConfig(
            String entityName, 
            String shardKey, 
            int retentionDays) {
        
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        
        // Generate actual data nodes (table names) for retention window
        String actualDataNodes = generateActualDataNodes(entityName, retentionDays);
        
        // Create table rule
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(
            entityName,
            actualDataNodes
        );
        
        // Set complex sharding strategy for range query support
        String algorithmName = entityName + "_date_complex";
        tableRule.setTableShardingStrategy(new ComplexShardingStrategyConfiguration(
            shardKey,
            algorithmName
        ));
        
        shardingRuleConfig.getTables().add(tableRule);
        
        // Create custom complex sharding algorithm for date-based routing with range query support
        Properties algorithmProps = new Properties();
        algorithmProps.setProperty("table-prefix", entityName);
        algorithmProps.setProperty("sharding-column", shardKey);
        algorithmProps.setProperty("hourly-sharding", "false"); // MySQL handles partitioning
        
        shardingRuleConfig.getShardingAlgorithms().put(
            algorithmName,
            new AlgorithmConfiguration("DATE_BASED_COMPLEX", algorithmProps)
        );
        
        return shardingRuleConfig;
    }
    
    /**
     * Generate actual data nodes string for ShardingSphere
     * Example: "ds0.sms_20250720,ds0.sms_20250721,ds0.sms_20250722"
     */
    private static String generateActualDataNodes(String entityName, int retentionDays) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(retentionDays);
        LocalDateTime endDate = now.plusDays(retentionDays);
        
        List<String> nodes = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        
        // Daily tables with MySQL native hourly partitioning
        while (!current.isAfter(endDate)) {
            String tableName = entityName + "_" + 
                current.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            nodes.add("ds0." + tableName);
            current = current.plusDays(1);
        }
        
        String result = String.join(",", nodes);
        System.out.println("Generated actual data nodes: " + result);
        return result;
    }
}