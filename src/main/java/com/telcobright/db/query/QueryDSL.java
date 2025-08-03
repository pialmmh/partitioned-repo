package com.telcobright.db.query;

import com.telcobright.db.repository.PartitionedQueryBuilder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Type-safe DSL for building partitioned queries
 * 
 * Example usage:
 * <pre>
 * String query = QueryDSL.select()
 *     .column("user_id")
 *     .count("*", "count")
 *     .max("created_at", "max_created_at")
 *     .min("created_at", "min_created_at")
 *     .from("sms")
 *     .where(w -> w
 *         .dateRange("created_at", startDate, endDate)
 *         .and("status", Operator.EQUALS, "SENT"))
 *     .groupBy("user_id")
 *     .orderBy("count", Order.DESC)
 *     .build();
 * </pre>
 */
public class QueryDSL {
    
    private QueryDSL() {} // Static factory methods only
    
    /**
     * Start building a SELECT query
     */
    public static SelectBuilder select() {
        return new SelectBuilder();
    }
    
    /**
     * Operator types for WHERE conditions
     */
    public enum Operator {
        EQUALS("="),
        NOT_EQUALS("!="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUALS(">="),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUALS("<="),
        LIKE("LIKE"),
        IN("IN"),
        NOT_IN("NOT IN"),
        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL");
        
        private final String sql;
        
        Operator(String sql) {
            this.sql = sql;
        }
        
        public String toSql() {
            return sql;
        }
    }
    
    /**
     * Order direction for ORDER BY
     */
    public enum Order {
        ASC("ASC"),
        DESC("DESC");
        
        private final String sql;
        
        Order(String sql) {
            this.sql = sql;
        }
        
        public String toSql() {
            return sql;
        }
    }
    
    /**
     * Aggregate function types
     */
    public enum AggregateFunction {
        COUNT("COUNT"),
        SUM("SUM"),
        AVG("AVG"),
        MAX("MAX"),
        MIN("MIN");
        
        private final String sql;
        
        AggregateFunction(String sql) {
            this.sql = sql;
        }
        
        public String toSql() {
            return sql;
        }
    }
    
    /**
     * Builder for SELECT clause
     */
    public static class SelectBuilder {
        private final List<SelectColumn> columns = new ArrayList<>();
        
        /**
         * Add a regular column
         */
        public SelectBuilder column(String name) {
            columns.add(new SelectColumn(name, null, null, null));
            return this;
        }
        
        /**
         * Add a column with alias
         */
        public SelectBuilder column(String name, String alias) {
            columns.add(new SelectColumn(name, null, null, alias));
            return this;
        }
        
        /**
         * Add COUNT aggregate
         */
        public SelectBuilder count(String expression, String alias) {
            columns.add(new SelectColumn(expression, AggregateFunction.COUNT, null, alias));
            return this;
        }
        
        /**
         * Add COUNT(*) 
         */
        public SelectBuilder countAll(String alias) {
            return count("*", alias);
        }
        
        /**
         * Add SUM aggregate
         */
        public SelectBuilder sum(String column, String alias) {
            columns.add(new SelectColumn(column, AggregateFunction.SUM, null, alias));
            return this;
        }
        
        /**
         * Add AVG aggregate
         */
        public SelectBuilder avg(String column, String alias) {
            columns.add(new SelectColumn(column, AggregateFunction.AVG, null, alias));
            return this;
        }
        
        /**
         * Add MAX aggregate
         */
        public SelectBuilder max(String column, String alias) {
            columns.add(new SelectColumn(column, AggregateFunction.MAX, null, alias));
            return this;
        }
        
        /**
         * Add MIN aggregate
         */
        public SelectBuilder min(String column, String alias) {
            columns.add(new SelectColumn(column, AggregateFunction.MIN, null, alias));
            return this;
        }
        
        /**
         * Add custom aggregate with DISTINCT
         */
        public SelectBuilder aggregate(AggregateFunction func, String expression, String alias, boolean distinct) {
            columns.add(new SelectColumn(expression, func, distinct, alias));
            return this;
        }
        
        /**
         * Specify FROM table
         */
        public FromBuilder from(String tableName) {
            return new FromBuilder(columns, tableName);
        }
    }
    
    /**
     * Builder for FROM clause and subsequent query parts
     */
    public static class FromBuilder {
        private final List<SelectColumn> columns;
        private final String tableName;
        private WhereClause whereClause;
        private final List<String> groupByColumns = new ArrayList<>();
        private final List<OrderByColumn> orderByColumns = new ArrayList<>();
        private Integer limit;
        
        FromBuilder(List<SelectColumn> columns, String tableName) {
            this.columns = columns;
            this.tableName = tableName;
        }
        
        /**
         * Add WHERE conditions
         */
        public FromBuilder where(java.util.function.Function<WhereBuilder, WhereBuilder> whereFunc) {
            WhereBuilder whereBuilder = new WhereBuilder();
            whereBuilder = whereFunc.apply(whereBuilder);
            this.whereClause = whereBuilder.build();
            return this;
        }
        
        /**
         * Add GROUP BY columns
         */
        public FromBuilder groupBy(String... columns) {
            groupByColumns.addAll(Arrays.asList(columns));
            return this;
        }
        
        /**
         * Add ORDER BY
         */
        public FromBuilder orderBy(String column, Order order) {
            orderByColumns.add(new OrderByColumn(column, order));
            return this;
        }
        
        /**
         * Add ORDER BY ascending
         */
        public FromBuilder orderByAsc(String column) {
            return orderBy(column, Order.ASC);
        }
        
        /**
         * Add ORDER BY descending
         */
        public FromBuilder orderByDesc(String column) {
            return orderBy(column, Order.DESC);
        }
        
        /**
         * Add LIMIT
         */
        public FromBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }
        
        /**
         * Build the final SQL query
         */
        public String build() {
            return buildQuery(false);
        }
        
        /**
         * Build as partitioned query with UNION ALL
         */
        public String buildPartitioned(String database, LocalDateTime startDate, LocalDateTime endDate) {
            // Validate that we have a date range in WHERE clause
            if (whereClause == null || whereClause.dateRangeField == null) {
                throw new IllegalStateException("Partitioned query requires a date range in WHERE clause");
            }
            
            return PartitionedQueryBuilder.getQuery(buildQuery(true), tableName, database);
        }
        
        private String buildQuery(boolean includeTrailingSemicolon) {
            StringBuilder sql = new StringBuilder("SELECT ");
            
            // Build SELECT clause
            sql.append(columns.stream()
                .map(this::formatSelectColumn)
                .collect(Collectors.joining(", ")));
            
            // FROM clause
            sql.append("\nFROM ").append(tableName);
            
            // WHERE clause
            if (whereClause != null) {
                sql.append("\nWHERE ").append(whereClause.toSql());
            }
            
            // GROUP BY clause
            if (!groupByColumns.isEmpty()) {
                sql.append("\nGROUP BY ").append(String.join(", ", groupByColumns));
            }
            
            // ORDER BY clause
            if (!orderByColumns.isEmpty()) {
                sql.append("\nORDER BY ");
                sql.append(orderByColumns.stream()
                    .map(o -> o.column + " " + o.order.toSql())
                    .collect(Collectors.joining(", ")));
            }
            
            // LIMIT clause
            if (limit != null) {
                sql.append("\nLIMIT ").append(limit);
            }
            
            if (includeTrailingSemicolon) {
                sql.append(";");
            }
            
            return sql.toString();
        }
        
        private String formatSelectColumn(SelectColumn col) {
            if (col.function == null) {
                // Regular column
                return col.alias != null ? col.name + " AS " + col.alias : col.name;
            } else {
                // Aggregate function
                String expr = col.function.toSql() + "(";
                if (col.distinct != null && col.distinct) {
                    expr += "DISTINCT ";
                }
                expr += col.name + ")";
                
                return col.alias != null ? expr + " AS " + col.alias : expr;
            }
        }
    }
    
    /**
     * Builder for WHERE conditions
     */
    public static class WhereBuilder {
        private final List<WhereCondition> conditions = new ArrayList<>();
        private String dateRangeField;
        private LocalDateTime startDate;
        private LocalDateTime endDate;
        
        /**
         * Add a date range condition
         */
        public WhereBuilder dateRange(String field, LocalDateTime start, LocalDateTime end) {
            this.dateRangeField = field;
            this.startDate = start;
            this.endDate = end;
            conditions.add(new WhereCondition(field, Operator.GREATER_THAN_OR_EQUALS, 
                "'" + formatDateTime(start) + "'", "AND"));
            conditions.add(new WhereCondition(field, Operator.LESS_THAN_OR_EQUALS, 
                "'" + formatDateTime(end) + "'", "AND"));
            return this;
        }
        
        /**
         * Add an equals condition
         */
        public WhereBuilder equals(String field, Object value) {
            return and(field, Operator.EQUALS, value);
        }
        
        /**
         * Add a NOT equals condition
         */
        public WhereBuilder notEquals(String field, Object value) {
            return and(field, Operator.NOT_EQUALS, value);
        }
        
        /**
         * Add an IN condition
         */
        public WhereBuilder in(String field, Object... values) {
            String valueList = Arrays.stream(values)
                .map(this::formatValue)
                .collect(Collectors.joining(", ", "(", ")"));
            conditions.add(new WhereCondition(field, Operator.IN, valueList, "AND"));
            return this;
        }
        
        /**
         * Add an IS NULL condition
         */
        public WhereBuilder isNull(String field) {
            conditions.add(new WhereCondition(field, Operator.IS_NULL, "", "AND"));
            return this;
        }
        
        /**
         * Add an IS NOT NULL condition
         */
        public WhereBuilder isNotNull(String field) {
            conditions.add(new WhereCondition(field, Operator.IS_NOT_NULL, "", "AND"));
            return this;
        }
        
        /**
         * Add a LIKE condition
         */
        public WhereBuilder like(String field, String pattern) {
            return and(field, Operator.LIKE, pattern);
        }
        
        /**
         * Add an AND condition
         */
        public WhereBuilder and(String field, Operator operator, Object value) {
            conditions.add(new WhereCondition(field, operator, formatValue(value), "AND"));
            return this;
        }
        
        /**
         * Add an OR condition
         */
        public WhereBuilder or(String field, Operator operator, Object value) {
            conditions.add(new WhereCondition(field, operator, formatValue(value), "OR"));
            return this;
        }
        
        private String formatValue(Object value) {
            if (value == null) {
                return "NULL";
            } else if (value instanceof String) {
                return "'" + value + "'";
            } else if (value instanceof LocalDateTime) {
                return "'" + formatDateTime((LocalDateTime) value) + "'";
            } else {
                return value.toString();
            }
        }
        
        private String formatDateTime(LocalDateTime dateTime) {
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        
        WhereClause build() {
            WhereClause clause = new WhereClause();
            clause.conditions = new ArrayList<>(conditions);
            clause.dateRangeField = dateRangeField;
            clause.startDate = startDate;
            clause.endDate = endDate;
            return clause;
        }
    }
    
    // Internal data structures
    
    private static class SelectColumn {
        final String name;
        final AggregateFunction function;
        final Boolean distinct;
        final String alias;
        
        SelectColumn(String name, AggregateFunction function, Boolean distinct, String alias) {
            this.name = name;
            this.function = function;
            this.distinct = distinct;
            this.alias = alias;
        }
    }
    
    private static class WhereCondition {
        final String field;
        final Operator operator;
        final String value;
        final String connector; // AND/OR
        
        WhereCondition(String field, Operator operator, String value, String connector) {
            this.field = field;
            this.operator = operator;
            this.value = value;
            this.connector = connector;
        }
    }
    
    private static class WhereClause {
        List<WhereCondition> conditions;
        String dateRangeField;
        LocalDateTime startDate;
        LocalDateTime endDate;
        
        String toSql() {
            if (conditions.isEmpty()) {
                return "";
            }
            
            StringBuilder sql = new StringBuilder();
            for (int i = 0; i < conditions.size(); i++) {
                WhereCondition cond = conditions.get(i);
                
                if (i > 0) {
                    sql.append(" ").append(cond.connector).append(" ");
                }
                
                sql.append(cond.field).append(" ").append(cond.operator.toSql());
                
                // Handle operators that don't need values
                if (cond.operator != Operator.IS_NULL && cond.operator != Operator.IS_NOT_NULL) {
                    sql.append(" ").append(cond.value);
                }
            }
            
            return sql.toString();
        }
    }
    
    private static class OrderByColumn {
        final String column;
        final Order order;
        
        OrderByColumn(String column, Order order) {
            this.column = column;
            this.order = order;
        }
    }
}