package com.telcobright.db.sharding;

import com.google.common.collect.Range;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Custom date-based complex sharding algorithm for range query support
 * Supports both single-value and range-based sharding for date columns
 */
public class DateBasedComplexShardingAlgorithm implements ComplexKeysShardingAlgorithm<Comparable<?>> {
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter DATE_HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HH");
    private String tablePrefix;
    private String shardingColumn;
    private boolean hourlySharding = false; // Flag to determine if hourly sharding is enabled
    
    @Override
    public void init(Properties props) {
        this.tablePrefix = props.getProperty("table-prefix", "default");
        this.shardingColumn = props.getProperty("sharding-column", "created_at");
        this.hourlySharding = "true".equalsIgnoreCase(props.getProperty("hourly-sharding", "false"));
    }
    
    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, ComplexKeysShardingValue<Comparable<?>> shardingValue) {
        Collection<String> result = new LinkedHashSet<>();
        
        // Get the range values for the sharding column (using Guava Range)
        Map<String, Range<Comparable<?>>> columnNameAndRangeValuesMap = shardingValue.getColumnNameAndRangeValuesMap();
        Map<String, Collection<Comparable<?>>> columnNameAndShardingValuesMap = shardingValue.getColumnNameAndShardingValuesMap();
        
        // Handle range queries (BETWEEN operations)
        if (columnNameAndRangeValuesMap.containsKey(shardingColumn)) {
            Range<Comparable<?>> range = columnNameAndRangeValuesMap.get(shardingColumn);
            result.addAll(getTablesForRange(availableTargetNames, range));
        }
        
        // Handle single value queries (= operations)
        if (columnNameAndShardingValuesMap.containsKey(shardingColumn)) {
            Collection<Comparable<?>> shardingValues = columnNameAndShardingValuesMap.get(shardingColumn);
            result.addAll(getTablesForValues(availableTargetNames, shardingValues));
        }
        
        // If no specific routing found, return all tables (safe fallback)
        if (result.isEmpty()) {
            result.addAll(availableTargetNames);
        }
        
        return result;
    }
    
    /**
     * Get target tables for range values (BETWEEN queries)
     */
    private Collection<String> getTablesForRange(Collection<String> availableTargetNames, Range<Comparable<?>> range) {
        Set<String> result = new LinkedHashSet<>();
        
        if (range != null && range.hasLowerBound() && range.hasUpperBound()) {
            Comparable<?> start = range.lowerEndpoint();
            Comparable<?> end = range.upperEndpoint();
            
            LocalDateTime startDate = parseToLocalDateTime(start);
            LocalDateTime endDate = parseToLocalDateTime(end);
            
            if (startDate != null && endDate != null) {
                // Generate all dates between start and end
                LocalDateTime current = startDate.toLocalDate().atStartOfDay();
                LocalDateTime endDay = endDate.toLocalDate().atStartOfDay().plusDays(1);
                
                if (hourlySharding) {
                    // For hourly sharding: generate tables for each hour in the range
                    while (current.isBefore(endDay)) {
                        for (int hour = 0; hour < 24; hour++) {
                            LocalDateTime hourlyDateTime = current.withHour(hour);
                            if (hourlyDateTime.isAfter(endDate) || hourlyDateTime.isBefore(startDate)) {
                                continue;
                            }
                            
                            String tableSuffix = hourlyDateTime.format(DATE_HOUR_FORMATTER);
                            String targetTable = tablePrefix + "_" + tableSuffix;
                            
                            if (availableTargetNames.contains(targetTable)) {
                                result.add(targetTable);
                            }
                        }
                        current = current.plusDays(1);
                    }
                } else {
                    // For daily sharding: generate one table per day
                    while (current.isBefore(endDay)) {
                        String tableSuffix = current.format(DATE_FORMATTER);
                        String targetTable = tablePrefix + "_" + tableSuffix;
                        
                        if (availableTargetNames.contains(targetTable)) {
                            result.add(targetTable);
                        }
                        
                        current = current.plusDays(1);
                    }
                }
            }
        }
        
        return result;
    }
    
    /**
     * Get target tables for specific values (= queries)
     */
    private Collection<String> getTablesForValues(Collection<String> availableTargetNames, Collection<Comparable<?>> values) {
        Set<String> result = new LinkedHashSet<>();
        
        for (Comparable<?> value : values) {
            LocalDateTime dateTime = parseToLocalDateTime(value);
            if (dateTime != null) {
                String tableSuffix = hourlySharding ? 
                    dateTime.format(DATE_HOUR_FORMATTER) : 
                    dateTime.format(DATE_FORMATTER);
                String targetTable = tablePrefix + "_" + tableSuffix;
                
                if (availableTargetNames.contains(targetTable)) {
                    result.add(targetTable);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Parse various date formats to LocalDateTime
     */
    private LocalDateTime parseToLocalDateTime(Comparable<?> value) {
        if (value == null) {
            return null;
        }
        
        try {
            if (value instanceof LocalDateTime) {
                return (LocalDateTime) value;
            } else if (value instanceof java.sql.Timestamp) {
                return ((java.sql.Timestamp) value).toLocalDateTime();
            } else if (value instanceof java.util.Date) {
                return LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), 
                    java.time.ZoneId.systemDefault());
            } else if (value instanceof String) {
                String str = (String) value;
                // Try parsing yyyy-MM-dd HH:mm:ss format
                if (str.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")) {
                    return LocalDateTime.parse(str.substring(0, 19), 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
                // Try parsing yyyy-MM-dd format
                else if (str.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return LocalDateTime.parse(str + " 00:00:00", 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
            }
        } catch (Exception e) {
            // Log warning but don't fail
            System.err.println("Warning: Could not parse date value: " + value + " (" + e.getMessage() + ")");
        }
        
        return null;
    }
    
    @Override
    public String getType() {
        return "DATE_BASED_COMPLEX";
    }
    
    @Override
    public boolean isDefault() {
        return false;
    }
}