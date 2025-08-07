package com.telcobright.db.repository;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Advanced query builder that transforms queries for partitioned tables with intelligent aggregation
 * Handles different aggregate functions with proper outer/inner aggregation mapping
 */
public class PartitionedQueryBuilder {
    
    private static final DateTimeFormatter TABLE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter SQL_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Aggregation mapping: inner function -> outer function
    private static final Map<String, String> AGGREGATION_MAPPING = Map.of(
        "COUNT", "SUM",
        "SUM", "SUM",
        "MAX", "MAX",
        "MIN", "MIN",
        "AVG", "AVG"  // Note: AVG requires special handling for accuracy
    );
    
    /**
     * Transforms a query on logical table to UNION ALL across daily partitioned tables
     * with intelligent date range splitting and aggregation handling
     */
    public static String getQuery(String originalQuery, String tableName, String database) {
        return getQuery(originalQuery, tableName, database, "created_at");
    }
    
    /**
     * Transforms a query on logical table to UNION ALL across daily partitioned tables
     * with intelligent date range splitting and aggregation handling
     */
    public static String getQuery(String originalQuery, String tableName, String database, String shardingColumn) {
        // Parse the query components
        QueryComponents components = parseQuery(originalQuery);
        if (components == null) {
            return originalQuery; // Return original if parsing fails
        }
        
        // Extract date range from WHERE clause
        DateRange dateRange = extractDateRange(components.whereClause, shardingColumn);
        if (dateRange == null) {
            return originalQuery; // No date range found
        }
        
        // Generate daily partitions with adjusted date ranges
        List<PartitionInfo> partitions = generatePartitions(dateRange, tableName, database);
        
        // Filter to only existing tables - if no tables exist, return empty result
        partitions = filterExistingTables(partitions);
        if (partitions.isEmpty()) {
            return "SELECT " + components.selectClause + " FROM (SELECT NULL WHERE FALSE) empty LIMIT 0";
        }
        
        // Build the transformed query
        return buildTransformedQuery(components, partitions, shardingColumn);
    }
    
    /**
     * Parse SQL query into components
     */
    private static QueryComponents parseQuery(String query) {
        try {
            // Normalize query
            String normalized = query.replaceAll("\\s+", " ").trim();
            
            // Extract SELECT clause
            Pattern selectPattern = Pattern.compile("SELECT\\s+(.+?)\\s+FROM", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            Matcher selectMatcher = selectPattern.matcher(normalized);
            if (!selectMatcher.find()) return null;
            String selectClause = selectMatcher.group(1).trim();
            
            // Extract FROM table
            Pattern fromPattern = Pattern.compile("FROM\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
            Matcher fromMatcher = fromPattern.matcher(normalized);
            if (!fromMatcher.find()) return null;
            String fromTable = fromMatcher.group(1).trim();
            
            // Extract WHERE clause
            Pattern wherePattern = Pattern.compile("WHERE\\s+(.+?)(?:GROUP\\s+BY|ORDER\\s+BY|$)", Pattern.CASE_INSENSITIVE);
            Matcher whereMatcher = wherePattern.matcher(normalized);
            String whereClause = whereMatcher.find() ? whereMatcher.group(1).trim() : "";
            
            // Extract GROUP BY
            Pattern groupByPattern = Pattern.compile("GROUP\\s+BY\\s+(.+?)(?:ORDER\\s+BY|$)", Pattern.CASE_INSENSITIVE);
            Matcher groupByMatcher = groupByPattern.matcher(normalized);
            String groupByClause = groupByMatcher.find() ? groupByMatcher.group(1).trim() : "";
            
            // Extract ORDER BY
            Pattern orderByPattern = Pattern.compile("ORDER\\s+BY\\s+(.+?)(?:;|$)", Pattern.CASE_INSENSITIVE);
            Matcher orderByMatcher = orderByPattern.matcher(normalized);
            String orderByClause = orderByMatcher.find() ? orderByMatcher.group(1).trim().replaceAll(";$", "") : "";
            
            return new QueryComponents(selectClause, fromTable, whereClause, groupByClause, orderByClause);
        } catch (Exception e) {
            System.err.println("Error parsing query: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Extract date range from WHERE clause
     */
    private static DateRange extractDateRange(String whereClause, String shardingColumn) {
        try {
            // Pattern for date comparisons with dynamic column name
            String patternStr = shardingColumn + "\\s*([><=]+)\\s*'([^']+)'";
            Pattern datePattern = Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
            Matcher matcher = datePattern.matcher(whereClause);
            
            LocalDateTime startDate = null;
            LocalDateTime endDate = null;
            
            while (matcher.find()) {
                String operator = matcher.group(1);
                String dateStr = matcher.group(2);
                LocalDateTime date = LocalDateTime.parse(dateStr, SQL_DATE_FORMAT);
                
                if (operator.contains(">")) {
                    startDate = date;
                } else if (operator.contains("<")) {
                    endDate = date;
                }
            }
            
            return (startDate != null && endDate != null) ? new DateRange(startDate, endDate) : null;
        } catch (Exception e) {
            System.err.println("Error extracting date range: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Generate partition information with adjusted date ranges
     */
    private static List<PartitionInfo> generatePartitions(DateRange range, String tableName, String database) {
        List<PartitionInfo> partitions = new ArrayList<>();
        
        LocalDate currentDate = range.start.toLocalDate();
        LocalDate endDate = range.end.toLocalDate();
        
        while (!currentDate.isAfter(endDate)) {
            // Calculate partition-specific date range
            LocalDateTime partitionStart = LocalDateTime.of(currentDate, java.time.LocalTime.MIN);
            LocalDateTime partitionEnd = LocalDateTime.of(currentDate.plusDays(1), java.time.LocalTime.MIN);
            
            // Adjust for actual query range
            if (partitionStart.isBefore(range.start)) {
                partitionStart = range.start;
            }
            if (partitionEnd.isAfter(range.end)) {
                partitionEnd = range.end;
            }
            
            String partitionTable = String.format("%s.%s_%s", 
                database, tableName, currentDate.format(TABLE_DATE_FORMAT));
            
            partitions.add(new PartitionInfo(partitionTable, partitionStart, partitionEnd));
            currentDate = currentDate.plusDays(1);
        }
        
        return partitions;
    }
    
    /**
     * Filter partitions to only include tables that actually exist
     */
    private static List<PartitionInfo> filterExistingTables(List<PartitionInfo> partitions) {
        List<PartitionInfo> existingPartitions = new ArrayList<>();
        
        for (PartitionInfo partition : partitions) {
            if (tableExists(partition.tableName)) {
                existingPartitions.add(partition);
            }
        }
        
        return existingPartitions;
    }
    
    /**
     * Check if a table exists
     */
    private static boolean tableExists(String fullTableName) {
        // Parse database.table format
        String[] parts = fullTableName.split("\\.");
        if (parts.length != 2) return false;
        
        String database = parts[0];
        String tableName = parts[1];
        
        try (Connection conn = createConnection(database)) {
            String query = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, database);
                stmt.setString(2, tableName);
                try (ResultSet rs = stmt.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            System.err.println("Error checking table existence for " + fullTableName + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Create database connection - reuses same logic as repositories
     */
    private static Connection createConnection(String database) throws SQLException {
        // Try MariaDB URL first, then MySQL - same as repository logic
        String mariadbUrl = String.format("jdbc:mariadb://127.0.0.1:3306/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", database);
        String mysqlUrl = String.format("jdbc:mysql://127.0.0.1:3306/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", database);
        
        try {
            return DriverManager.getConnection(mariadbUrl, "root", "123456");
        } catch (SQLException e) {
            // Fallback to MySQL URL
            return DriverManager.getConnection(mysqlUrl, "root", "123456");
        }
    }
    
    /**
     * Build the transformed query with UNION ALL
     */
    private static String buildTransformedQuery(QueryComponents components, List<PartitionInfo> partitions, String shardingColumn) {
        StringBuilder query = new StringBuilder();
        
        // Parse and transform SELECT columns
        List<SelectColumn> columns = parseSelectColumns(components.selectClause);
        
        // Build outer SELECT with transformed aggregations
        query.append("SELECT ");
        query.append(buildOuterSelect(columns));
        query.append("\nFROM\n(\n");
        
        // Build UNION ALL subqueries
        for (int i = 0; i < partitions.size(); i++) {
            if (i > 0) {
                query.append("\nUNION ALL \n");
            }
            
            PartitionInfo partition = partitions.get(i);
            query.append("SELECT ").append(components.selectClause).append("\n");
            query.append("FROM ").append(partition.tableName).append("\n");
            query.append("WHERE ").append(shardingColumn).append(" >='").append(partition.startDate.format(SQL_DATE_FORMAT));
            query.append("' AND ").append(shardingColumn).append(" <='").append(partition.endDate.format(SQL_DATE_FORMAT)).append("'");
            
            if (!components.groupByClause.isEmpty()) {
                query.append("\nGROUP BY ").append(components.groupByClause);
            }
        }
        
        query.append("\n) unioned");
        
        // Add outer GROUP BY - transform to use aliases
        if (!components.groupByClause.isEmpty()) {
            String outerGroupBy = transformGroupByForOuter(components.groupByClause, columns);
            query.append("\nGROUP BY ").append(outerGroupBy);
        }
        
        // Add ORDER BY if present
        if (!components.orderByClause.isEmpty()) {
            query.append("\nORDER BY ").append(components.orderByClause);
        }
        
        query.append(";");
        
        return query.toString();
    }
    
    /**
     * Parse SELECT columns to identify aggregations
     */
    private static List<SelectColumn> parseSelectColumns(String selectClause) {
        List<SelectColumn> columns = new ArrayList<>();
        
        // Split by comma but respect parentheses
        String[] parts = selectClause.split(",(?![^(]*\\))");
        
        for (String part : parts) {
            part = part.trim();
            
            // Check for aggregate functions - handle nested parentheses
            Pattern aggPattern = Pattern.compile("^(COUNT|SUM|MAX|MIN|AVG)\\s*\\(", 
                Pattern.CASE_INSENSITIVE);
            Matcher aggMatcher = aggPattern.matcher(part);
            
            if (aggMatcher.find()) {
                String function = aggMatcher.group(1).toUpperCase();
                
                // Find matching closing parenthesis
                int openParen = part.indexOf('(', aggMatcher.end() - 1);
                int closeParen = findMatchingParen(part, openParen);
                
                if (closeParen == -1) {
                    continue;
                }
                
                String expression = part.substring(openParen + 1, closeParen);
                
                // Extract alias if present
                String remaining = part.substring(closeParen + 1).trim();
                String alias = null;
                Pattern aliasPattern = Pattern.compile("^\\s*AS\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
                Matcher aliasMatcher = aliasPattern.matcher(remaining);
                if (aliasMatcher.find()) {
                    alias = aliasMatcher.group(1);
                }
                
                // If no alias, generate one
                if (alias == null || alias.trim().isEmpty()) {
                    alias = function.toLowerCase() + "_" + expression.replaceAll("[^\\w]", "_").replaceAll("_{2,}", "_").replaceAll("^_|_$", "");
                }
                
                columns.add(new SelectColumn(part, function, alias, true));
            } else {
                // Non-aggregate column
                String alias = part;
                if (part.contains(" AS ")) {
                    String[] aliasParts = part.split("\\s+AS\\s+", 2);
                    alias = aliasParts[1].trim();
                }
                columns.add(new SelectColumn(part, null, alias, false));
            }
        }
        
        return columns;
    }
    
    /**
     * Build outer SELECT with transformed aggregations
     */
    private static String buildOuterSelect(List<SelectColumn> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        
        for (SelectColumn col : columns) {
            if (col.isAggregate) {
                // Transform aggregation based on mapping
                String outerFunction = AGGREGATION_MAPPING.getOrDefault(col.aggregateFunction, col.aggregateFunction);
                
                if ("COUNT".equals(col.aggregateFunction)) {
                    // COUNT -> SUM
                    joiner.add(String.format("SUM(%s) AS %s", col.alias, col.alias));
                } else {
                    // Others maintain same function
                    joiner.add(String.format("%s(%s) AS %s", outerFunction, col.alias, col.alias));
                }
            } else {
                // Non-aggregate columns stay the same
                joiner.add(col.alias);
            }
        }
        
        return joiner.toString();
    }
    
    /**
     * Transform GROUP BY clause for outer query to use aliases instead of original expressions
     */
    private static String transformGroupByForOuter(String groupByClause, List<SelectColumn> columns) {
        String transformed = groupByClause;
        
        // Find non-aggregate columns that might be used in GROUP BY
        for (SelectColumn col : columns) {
            if (!col.isAggregate && col.alias != null) {
                // Replace the original expression with the alias
                // This handles cases like "DATE_FORMAT(created_at, '%Y-%m-%d %H:00')" -> "hour"
                String originalExpr = col.original;
                if (originalExpr.contains(" AS ")) {
                    String expr = originalExpr.substring(0, originalExpr.indexOf(" AS ")).trim();
                    transformed = transformed.replace(expr, col.alias);
                }
            }
        }
        
        return transformed;
    }
    
    /**
     * Find matching closing parenthesis, handling nested parentheses
     */
    private static int findMatchingParen(String str, int openPos) {
        int count = 1;
        for (int i = openPos + 1; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') {
                count++;
            } else if (c == ')') {
                count--;
                if (count == 0) {
                    return i;
                }
            }
        }
        return -1; // No matching closing parenthesis found
    }
    
    // Helper classes
    private static class QueryComponents {
        final String selectClause;
        final String fromTable;
        final String whereClause;
        final String groupByClause;
        final String orderByClause;
        
        QueryComponents(String selectClause, String fromTable, String whereClause, 
                       String groupByClause, String orderByClause) {
            this.selectClause = selectClause;
            this.fromTable = fromTable;
            this.whereClause = whereClause;
            this.groupByClause = groupByClause;
            this.orderByClause = orderByClause;
        }
    }
    
    private static class DateRange {
        final LocalDateTime start;
        final LocalDateTime end;
        
        DateRange(LocalDateTime start, LocalDateTime end) {
            this.start = start;
            this.end = end;
        }
    }
    
    private static class PartitionInfo {
        final String tableName;
        final LocalDateTime startDate;
        final LocalDateTime endDate;
        
        PartitionInfo(String tableName, LocalDateTime startDate, LocalDateTime endDate) {
            this.tableName = tableName;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }
    
    private static class SelectColumn {
        final String original;
        final String aggregateFunction;
        final String alias;
        final boolean isAggregate;
        
        SelectColumn(String original, String aggregateFunction, String alias, boolean isAggregate) {
            this.original = original;
            this.aggregateFunction = aggregateFunction;
            this.alias = alias;
            this.isAggregate = isAggregate;
        }
    }
    
    /**
     * Test the query builder
     */
    public static void main(String[] args) {
        String originalQuery = """
            select user_id, count(*) as count, max(created_at) as max_created_at, min(created_at) as min_created_at
            from sms
            where created_at >='2025-07-27 06:00:00' and created_at <='2025-07-29 13:00:00'
            group by user_id;
            """;
        
        System.out.println("Original Query:");
        System.out.println(originalQuery);
        System.out.println("\nTransformed Query:");
        System.out.println(getQuery(originalQuery, "sms", "test"));
    }
}