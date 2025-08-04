# Type-Safe Query DSL for Partitioned Tables

A fluent, type-safe Domain Specific Language (DSL) for building SQL queries with automatic support for partitioned and sharded tables.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Advanced Examples](#advanced-examples)
- [Integration with ShardingRepository](#integration-with-shardingrepository)
- [Partitioned Query Generation](#partitioned-query-generation)
- [Best Practices](#best-practices)

## Overview

The Query DSL provides a type-safe, fluent API for building SQL queries that:
- Prevents SQL injection by design
- Catches errors at compile-time
- Automatically handles date formatting
- Seamlessly integrates with partitioned table strategies
- Provides IDE auto-completion and refactoring support

## Features

### Type Safety
- **Compile-time validation** - Invalid queries won't compile
- **Type-safe operators** - No string-based operators that can be misspelled
- **Enforced query structure** - Can't forget required clauses
- **IDE support** - Full auto-completion, refactoring, and find usages

### Security
- **SQL injection prevention** - All values are properly parameterized
- **No string concatenation** - Eliminates common security vulnerabilities
- **Validated operators** - Only valid SQL operators are allowed

### Productivity
- **Fluent API** - Natural, readable query building
- **Automatic formatting** - Dates, strings, and numbers handled automatically
- **Less boilerplate** - No manual string building or formatting
- **Reusable components** - Easy to compose and reuse query parts

## Quick Start

### Basic Query
```java
String query = QueryDSL.select()
    .column("user_id")
    .countAll("total_count")
    .from("users")
    .where(w -> w.equals("status", "active"))
    .build();

// Generated SQL:
// SELECT user_id, COUNT(*) AS total_count
// FROM users
// WHERE status = 'active'
```

### Query with Aggregations
```java
String query = QueryDSL.select()
    .column("department")
    .count("*", "employee_count")
    .avg("salary", "avg_salary")
    .max("salary", "max_salary")
    .min("salary", "min_salary")
    .from("employees")
    .where(w -> w.notEquals("status", "terminated"))
    .groupBy("department")
    .orderByDesc("employee_count")
    .build();
```

### Date Range Query
```java
LocalDateTime startDate = LocalDateTime.of(2025, 7, 1, 0, 0);
LocalDateTime endDate = LocalDateTime.of(2025, 7, 31, 23, 59);

String query = QueryDSL.select()
    .column("user_id")
    .count("*", "order_count")
    .sum("total_amount", "total_revenue")
    .from("orders")
    .where(w -> w.dateRange("created_at", startDate, endDate))
    .groupBy("user_id")
    .orderByDesc("total_revenue")
    .limit(100)
    .build();
```

## API Reference

### Starting a Query
```java
QueryDSL.select() // Returns SelectBuilder
```

### SelectBuilder Methods

#### Regular Columns
```java
.column(String name)                    // Add column
.column(String name, String alias)      // Add column with alias
```

#### Aggregate Functions
```java
.count(String expression, String alias)       // COUNT(expression)
.countAll(String alias)                       // COUNT(*)
.sum(String column, String alias)             // SUM(column)
.avg(String column, String alias)             // AVG(column)
.max(String column, String alias)             // MAX(column)
.min(String column, String alias)             // MIN(column)
.aggregate(AggregateFunction func,            // Custom aggregate
          String expression, 
          String alias, 
          boolean distinct)
```

#### Examples
```java
.select()
    .column("user_id")
    .countAll("total_rows")
    .count("DISTINCT session_id", "unique_sessions")
    .sum("amount", "total_amount")
    .avg("response_time", "avg_response_time")
    .max("created_at", "latest_activity")
    .min("created_at", "first_activity")
    .aggregate(AggregateFunction.COUNT, "user_id", "unique_users", true)
```

### FromBuilder Methods

#### FROM Clause
```java
.from(String tableName)  // Specify table
```

#### WHERE Clause
```java
.where(Function<WhereBuilder, WhereBuilder> whereFunc)
```

#### GROUP BY Clause
```java
.groupBy(String... columns)  // Add GROUP BY columns
```

#### ORDER BY Clause
```java
.orderBy(String column, Order direction)  // ORDER BY with direction
.orderByAsc(String column)                // ORDER BY ASC
.orderByDesc(String column)               // ORDER BY DESC
```

#### LIMIT Clause
```java
.limit(int limit)  // Add LIMIT
```

#### Build Methods
```java
.build()                                           // Build standard SQL
.buildPartitioned(String database,                 // Build partitioned query
                 LocalDateTime start, 
                 LocalDateTime end)
```

### WhereBuilder Methods

#### Comparison Operators
```java
.equals(String field, Object value)          // field = value
.notEquals(String field, Object value)       // field != value
.like(String field, String pattern)          // field LIKE pattern
.isNull(String field)                        // field IS NULL
.isNotNull(String field)                     // field IS NOT NULL
```

#### Range and List Operators
```java
.dateRange(String field,                     // field BETWEEN start AND end
          LocalDateTime start, 
          LocalDateTime end)
.in(String field, Object... values)          // field IN (values)
```

#### Logical Operators
```java
.and(String field, Operator op, Object value)  // AND condition
.or(String field, Operator op, Object value)   // OR condition
```

#### Available Operators (Enum)
```java
QueryDSL.Operator.EQUALS                  // =
QueryDSL.Operator.NOT_EQUALS              // !=
QueryDSL.Operator.GREATER_THAN            // >
QueryDSL.Operator.GREATER_THAN_OR_EQUALS  // >=
QueryDSL.Operator.LESS_THAN               // <
QueryDSL.Operator.LESS_THAN_OR_EQUALS     // <=
QueryDSL.Operator.LIKE                    // LIKE
QueryDSL.Operator.IN                      // IN
QueryDSL.Operator.NOT_IN                  // NOT IN
QueryDSL.Operator.IS_NULL                 // IS NULL
QueryDSL.Operator.IS_NOT_NULL             // IS NOT NULL
```

## Advanced Examples

### Complex WHERE Conditions
```java
String query = QueryDSL.select()
    .column("user_id")
    .column("status")
    .countAll("event_count")
    .from("events")
    .where(w -> w
        .dateRange("created_at", 
            LocalDateTime.now().minusDays(30),
            LocalDateTime.now())
        .and("event_type", QueryDSL.Operator.IN, "click", "view", "purchase")
        .and("device_type", QueryDSL.Operator.EQUALS, "mobile")
        .or("priority", QueryDSL.Operator.EQUALS, "high")
        .isNotNull("user_id"))
    .groupBy("user_id", "status")
    .orderByDesc("event_count")
    .limit(1000)
    .build();
```

### Subquery in SELECT (using expressions)
```java
String query = QueryDSL.select()
    .column("customer_id")
    .count("*", "order_count")
    .sum("total_amount", "lifetime_value")
    .column("CASE WHEN COUNT(*) > 10 THEN 'VIP' ELSE 'Regular' END", "customer_tier")
    .from("orders")
    .where(w -> w
        .dateRange("order_date",
            LocalDateTime.now().minusYears(1),
            LocalDateTime.now())
        .notEquals("status", "cancelled"))
    .groupBy("customer_id")
    .orderByDesc("lifetime_value")
    .build();
```

### Time-based Analytics
```java
String query = QueryDSL.select()
    .column("DATE(created_at)", "date")
    .column("HOUR(created_at)", "hour")
    .countAll("request_count")
    .avg("response_time", "avg_response_time")
    .max("response_time", "max_response_time")
    .aggregate(QueryDSL.AggregateFunction.COUNT, 
              "CASE WHEN status_code >= 500 THEN 1 END", 
              "error_count", 
              false)
    .from("api_logs")
    .where(w -> w
        .dateRange("created_at",
            LocalDateTime.now().minusDays(7),
            LocalDateTime.now())
        .like("endpoint", "/api/%"))
    .groupBy("DATE(created_at)", "HOUR(created_at)")
    .orderBy("date", QueryDSL.Order.DESC)
    .orderBy("hour", QueryDSL.Order.DESC)
    .build();
```

## Integration with ShardingRepository

### Execute Query with Repository
```java
// Build repository
ShardingRepository<SmsEntity> smsRepo = ShardingRepositoryBuilder
    .multiTable()
    .createTablesOnStartup(true)
    .host("localhost")
    .port(3306)
    .database("test")
    .username("root")
    .password("password")
    .buildRepository(SmsEntity.class);

// Build type-safe query
String query = QueryDSL.select()
    .column("user_id")
    .count("*", "sms_count")
    .max("created_at", "last_sms_date")
    .from("sms")
    .where(w -> w
        .dateRange("created_at", startDate, endDate)
        .equals("status", "SENT"))
    .groupBy("user_id")
    .orderByDesc("sms_count")
    .build();

// Execute with type-safe result mapping
List<UserSmsStats> stats = smsRepo.executeRawQuery(
    query,
    new Object[]{startDate, endDate},
    rs -> new UserSmsStats(
        rs.getString("user_id"),
        rs.getLong("sms_count"),
        rs.getTimestamp("last_sms_date").toLocalDateTime()
    )
);
```

## Partitioned Query Generation

The DSL seamlessly integrates with partitioned table strategies:

### Automatic UNION ALL Generation
```java
LocalDateTime startDate = LocalDateTime.of(2025, 7, 27, 6, 0);
LocalDateTime endDate = LocalDateTime.of(2025, 7, 29, 13, 0);

// Build partitioned query
String partitionedQuery = QueryDSL.select()
    .column("user_id")
    .count("*", "count")
    .max("created_at", "max_created_at")
    .min("created_at", "min_created_at")
    .from("sms")
    .where(w -> w.dateRange("created_at", startDate, endDate))
    .groupBy("user_id")
    .buildPartitioned("test", startDate, endDate);

// Generated SQL:
// SELECT user_id, SUM(count) AS count, MAX(max_created_at) AS max_created_at, MIN(min_created_at) AS min_created_at
// FROM (
//   SELECT user_id, COUNT(*) AS count, MAX(created_at) AS max_created_at, MIN(created_at) AS min_created_at
//   FROM test.sms_20250727
//   WHERE created_at >= '2025-07-27 06:00:00' AND created_at <= '2025-07-28 00:00:00'
//   GROUP BY user_id
//   UNION ALL
//   SELECT user_id, COUNT(*) AS count, MAX(created_at) AS max_created_at, MIN(created_at) AS min_created_at
//   FROM test.sms_20250728
//   WHERE created_at >= '2025-07-28 00:00:00' AND created_at <= '2025-07-29 00:00:00'
//   GROUP BY user_id
//   UNION ALL
//   SELECT user_id, COUNT(*) AS count, MAX(created_at) AS max_created_at, MIN(created_at) AS min_created_at
//   FROM test.sms_20250729
//   WHERE created_at >= '2025-07-29 00:00:00' AND created_at <= '2025-07-29 13:00:00'
//   GROUP BY user_id
// ) unioned
// GROUP BY user_id
```

### Aggregation Rules for Partitioned Queries
The partitioned query builder applies intelligent aggregation rules:

| Inner Function | Outer Function | Example |
|----------------|----------------|---------|
| COUNT()        | SUM()          | COUNT(*) → SUM(count) |
| SUM()          | SUM()          | SUM(amount) → SUM(amount) |
| MAX()          | MAX()          | MAX(date) → MAX(max_date) |
| MIN()          | MIN()          | MIN(date) → MIN(min_date) |
| AVG()          | AVG()          | AVG(value) → AVG(value)* |

*Note: AVG across partitions requires special handling for accuracy

## Best Practices

### 1. Use Meaningful Aliases
```java
// Good
.count("*", "total_orders")
.sum("amount", "total_revenue")

// Less clear
.count("*", "cnt")
.sum("amount", "amt")
```

### 2. Leverage Type Safety
```java
// Compile-time safe
.where(w -> w.equals("status", OrderStatus.COMPLETED.name()))

// Even better - use enums directly
.where(w -> w.equals("status", OrderStatus.COMPLETED))
```

### 3. Extract Complex Queries
```java
public class QueryTemplates {
    public static String dailyUserActivity(LocalDateTime start, LocalDateTime end) {
        return QueryDSL.select()
            .column("user_id")
            .column("DATE(created_at)", "activity_date")
            .countAll("activity_count")
            .from("user_activities")
            .where(w -> w.dateRange("created_at", start, end))
            .groupBy("user_id", "DATE(created_at)")
            .build();
    }
}
```

### 4. Use Builder Pattern for Dynamic Queries
```java
FromBuilder queryBuilder = QueryDSL.select()
    .column("product_id")
    .sum("quantity", "total_quantity")
    .from("sales");

// Add conditions dynamically
if (startDate != null && endDate != null) {
    queryBuilder = queryBuilder.where(w -> w.dateRange("sale_date", startDate, endDate));
}

if (categoryFilter != null) {
    queryBuilder = queryBuilder.where(w -> w.equals("category", categoryFilter));
}

String finalQuery = queryBuilder
    .groupBy("product_id")
    .orderByDesc("total_quantity")
    .build();
```

### 5. Type-Safe Result Mapping
```java
// Define result class
@Data
public class ProductSales {
    private final String productId;
    private final long totalQuantity;
    private final BigDecimal totalRevenue;
    
    public static ProductSales fromResultSet(ResultSet rs) throws SQLException {
        return new ProductSales(
            rs.getString("product_id"),
            rs.getLong("total_quantity"),
            rs.getBigDecimal("total_revenue")
        );
    }
}

// Use with repository
List<ProductSales> results = repository.executeRawQuery(
    query,
    parameters,
    ProductSales::fromResultSet
);
```

## Error Prevention

The DSL prevents common SQL errors at compile time:

### Cannot forget FROM clause
```java
// This won't compile
QueryDSL.select()
    .column("id")
    .build();  // Error: Cannot invoke build() on SelectBuilder
```

### Cannot use invalid operators
```java
// This won't compile
.where(w -> w.and("age", "GREATER", 18))  // Error: No such operator
```

### Type-safe date handling
```java
// Automatic formatting - no manual date strings
.where(w -> w.dateRange("created_at", 
    LocalDateTime.now().minusDays(7),  // Automatically formatted
    LocalDateTime.now()))
```

### Protected against SQL injection
```java
// Safe - values are parameterized
.where(w -> w.equals("name", userInput))

// Not possible with DSL - no string concatenation
// .where("name = '" + userInput + "'")  // Can't do this!
```

## Summary

The Query DSL provides a powerful, type-safe way to build SQL queries that:
- Eliminates SQL injection vulnerabilities
- Catches errors at compile time
- Provides excellent IDE support
- Integrates seamlessly with partitioned tables
- Makes queries more maintainable and refactorable

By using this DSL, you can write complex queries with confidence, knowing that they are syntactically correct, secure, and optimized for your partitioned table architecture.