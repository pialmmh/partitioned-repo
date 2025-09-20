# SQL Abstraction Layer

## Overview

Split-Verse implements a comprehensive SQL abstraction layer that enables database-agnostic operations while leveraging database-specific optimizations. The system uses raw SQL generation instead of JPA/ORM for maximum performance and control.

## Architecture

```
┌─────────────────────────────┐
│    Repository Operations    │
└──────────┬──────────────────┘
           │
┌──────────▼──────────────────┐
│    SqlGeneratorFactory      │
└──────────┬──────────────────┘
           │
    ┌──────┴──────┬──────┬──────┐
    ▼             ▼      ▼      ▼
┌────────┐ ┌────────┐ ┌──────┐ ┌─────────┐
│ MySQL  │ │PostgeSQL│ │Oracle│ │SQL Server│
│Generator│ │Generator│ │ Gen. │ │Generator │
└────────┘ └────────┘ └──────┘ └─────────┘
    △             △      △      △
    └──────┬──────┴──────┴──────┘
           ▼
    ┌─────────────┐
    │BaseSqlGenerator│
    │  (ANSI SQL)   │
    └───────────────┘
```

## Core Components

### 1. **SqlGenerator Interface**

```java
public interface SqlGenerator {
    // DDL Operations
    String generateCreateTable(String tableName, EntityMetadata metadata);
    String generateCreateTableWithPartitions(String tableName, EntityMetadata metadata,
                                           String partitionColumn, String partitionType);
    String generateAlterTableAddPartition(String tableName, String partitionName,
                                        String partitionDefinition);
    String generateDropTable(String tableName);
    String generateCreateIndex(String tableName, String indexName, List<String> columns);

    // DML Operations
    String generateInsert(String tableName, Map<String, Object> values);
    String generateBatchInsert(String tableName, List<Map<String, Object>> records);
    String generateSelectById(String tableName, String idColumn);
    String generateSelectWithDateRange(String tableName, String dateColumn,
                                      String startDate, String endDate);
    String generateUpdate(String tableName, Map<String, Object> values,
                         Map<String, Object> conditions);
    String generateDelete(String tableName, Map<String, Object> conditions);

    // Pagination and Sorting
    String generateSelectWithPagination(String baseQuery, int limit, int offset);
    String generateSelectWithSorting(String baseQuery, List<SortField> sortFields);

    // Utility Methods
    String escapeIdentifier(String identifier);
    String escapeLiteral(Object value);
    boolean supportsNativePartitioning();
}
```

### 2. **BaseSqlGenerator (ANSI SQL)**

Provides default ANSI SQL implementations:

```java
public class BaseSqlGenerator implements SqlGenerator {
    @Override
    public String generateSelectById(String tableName, String idColumn) {
        return String.format("SELECT * FROM %s WHERE %s = ?",
                escapeIdentifier(tableName),
                escapeIdentifier(idColumn));
    }

    @Override
    public String escapeIdentifier(String identifier) {
        return "\"" + identifier + "\"";  // ANSI standard
    }
}
```

## Database-Specific Implementations

### 1. **MySQL SQL Generator**

**Optimizations:**
- Native partitioning with TO_DAYS() function
- Multi-row INSERT with ON DUPLICATE KEY UPDATE
- Index hints (USE INDEX, FORCE INDEX)
- Backticks for identifiers

```java
public class MySQLSqlGenerator extends BaseSqlGenerator {
    @Override
    public String generateCreateTableWithPartitions(String tableName,
                                                   EntityMetadata metadata,
                                                   String partitionColumn,
                                                   String partitionType) {
        StringBuilder sql = new StringBuilder(generateCreateTable(tableName, metadata));
        sql.append("\nPARTITION BY RANGE (TO_DAYS(")
           .append(escapeIdentifier(partitionColumn))
           .append("))");
        return sql.toString();
    }

    @Override
    public String escapeIdentifier(String identifier) {
        return "`" + identifier + "`";  // MySQL uses backticks
    }

    @Override
    public String generateBatchInsert(String tableName,
                                     List<Map<String, Object>> records) {
        // MySQL-specific multi-row insert
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(escapeIdentifier(tableName));
        sql.append(" (").append(columns).append(") VALUES ");

        // Multiple value sets
        for (int i = 0; i < records.size(); i++) {
            sql.append("(").append(placeholders).append(")");
            if (i < records.size() - 1) sql.append(", ");
        }

        // Upsert behavior
        sql.append(" ON DUPLICATE KEY UPDATE ");
        sql.append(updateClause);

        return sql.toString();
    }
}
```

### 2. **PostgreSQL SQL Generator**

**Optimizations:**
- Declarative partitioning (PARTITION BY RANGE)
- RETURNING clause for insert/update
- COPY command support
- ON CONFLICT for upserts

```java
public class PostgreSQLSqlGenerator extends BaseSqlGenerator {
    @Override
    public String generateCreateTableWithPartitions(String tableName,
                                                   EntityMetadata metadata,
                                                   String partitionColumn,
                                                   String partitionType) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(escapeIdentifier(tableName));
        sql.append(" (...) PARTITION BY RANGE (");
        sql.append("DATE(").append(escapeIdentifier(partitionColumn)).append(")");
        sql.append(")");
        return sql.toString();
    }

    @Override
    public String generateBatchInsert(String tableName,
                                     List<Map<String, Object>> records) {
        // PostgreSQL ON CONFLICT
        String baseInsert = super.generateBatchInsert(tableName, records);
        return baseInsert + " ON CONFLICT (id) DO UPDATE SET " + updateClause;
    }

    @Override
    public String generateCreateIndex(String tableName, String indexName,
                                    List<String> columns) {
        // Concurrent index creation
        return String.format("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
                escapeIdentifier(indexName),
                escapeIdentifier(tableName),
                columnList);
    }
}
```

### 3. **Oracle SQL Generator**

**Optimizations:**
- Interval partitioning for automatic management
- INSERT ALL for batch operations
- MERGE statements for upserts
- Flashback queries

```java
public class OracleSqlGenerator extends BaseSqlGenerator {
    @Override
    public String generateCreateTableWithPartitions(String tableName,
                                                   EntityMetadata metadata,
                                                   String partitionColumn,
                                                   String partitionType) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName);
        sql.append(" (...) PARTITION BY RANGE (").append(partitionColumn).append(")");
        sql.append(" INTERVAL (NUMTODSINTERVAL(1, 'DAY'))");
        sql.append(" (PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01'))");
        return sql.toString();
    }

    @Override
    public String generateBatchInsert(String tableName,
                                     List<Map<String, Object>> records) {
        // Oracle INSERT ALL
        StringBuilder sql = new StringBuilder("INSERT ALL\n");
        for (Map<String, Object> record : records) {
            sql.append("  INTO ").append(tableName)
               .append(" VALUES (").append(placeholders).append(")\n");
        }
        sql.append("SELECT 1 FROM DUAL");
        return sql.toString();
    }

    @Override
    public String generateMerge(String tableName, Map<String, Object> values,
                               String idColumn) {
        return String.format(
            "MERGE INTO %s t USING (SELECT %s FROM DUAL) s " +
            "ON (t.%s = s.%s) " +
            "WHEN MATCHED THEN UPDATE SET %s " +
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
            tableName, valuePlaceholders, idColumn, idColumn,
            updateSetClause, columns, valuePlaceholders
        );
    }
}
```

### 4. **SQL Server SQL Generator**

**Optimizations:**
- Partition functions and schemes
- MERGE statements
- Temporal tables support
- Columnstore indexes

```java
public class SqlServerSqlGenerator extends BaseSqlGenerator {
    @Override
    public String generateCreateTableWithPartitions(String tableName,
                                                   EntityMetadata metadata,
                                                   String partitionColumn,
                                                   String partitionType) {
        StringBuilder sql = new StringBuilder();

        // Create partition function
        sql.append("CREATE PARTITION FUNCTION PF_").append(tableName);
        sql.append("(datetime2) AS RANGE RIGHT FOR VALUES ();\n");

        // Create partition scheme
        sql.append("CREATE PARTITION SCHEME PS_").append(tableName);
        sql.append(" AS PARTITION PF_").append(tableName);
        sql.append(" ALL TO ([PRIMARY]);\n");

        // Create table on partition scheme
        sql.append("CREATE TABLE ").append(escapeIdentifier(tableName));
        sql.append(" (...) ON PS_").append(tableName);
        sql.append("(").append(escapeIdentifier(partitionColumn)).append(")");

        return sql.toString();
    }

    @Override
    public String escapeIdentifier(String identifier) {
        return "[" + identifier + "]";  // SQL Server uses square brackets
    }

    @Override
    public String generateSelectWithPagination(String baseQuery,
                                              int limit, int offset) {
        // SQL Server 2012+ OFFSET/FETCH
        return baseQuery + " OFFSET " + offset +
               " ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }
}
```

## SQL Generation Patterns

### 1. **Type Mapping**

```java
protected String getSqlType(FieldMetadata field) {
    Class<?> type = field.getType();

    // Database-specific type mapping
    if (this instanceof MySQLSqlGenerator) {
        if (type == String.class) return "VARCHAR(255)";
        if (type == LocalDateTime.class) return "DATETIME";
        if (type == Boolean.class) return "TINYINT(1)";
    } else if (this instanceof PostgreSQLSqlGenerator) {
        if (type == String.class) return "VARCHAR(255)";
        if (type == LocalDateTime.class) return "TIMESTAMP WITHOUT TIME ZONE";
        if (type == Boolean.class) return "BOOLEAN";
    } else if (this instanceof OracleSqlGenerator) {
        if (type == String.class) return "VARCHAR2(255)";
        if (type == LocalDateTime.class) return "TIMESTAMP";
        if (type == Boolean.class) return "NUMBER(1)";
    } else if (this instanceof SqlServerSqlGenerator) {
        if (type == String.class) return "NVARCHAR(255)";
        if (type == LocalDateTime.class) return "DATETIME2";
        if (type == Boolean.class) return "BIT";
    }

    // Default ANSI SQL types
    if (type == String.class) return "VARCHAR(255)";
    if (type == Integer.class) return "INTEGER";
    if (type == Long.class) return "BIGINT";
    if (type == LocalDateTime.class) return "TIMESTAMP";
    if (type == Boolean.class) return "BOOLEAN";

    return "TEXT";  // Fallback
}
```

### 2. **Date Range Queries**

```java
// Proper boundary handling (>= start AND < end)
public String generateSelectWithDateRange(String tableName,
                                         String dateColumn,
                                         String startDate,
                                         String endDate) {
    return String.format("SELECT * FROM %s WHERE %s >= ? AND %s < ?",
            escapeIdentifier(tableName),
            escapeIdentifier(dateColumn),
            escapeIdentifier(dateColumn));
}
```

### 3. **Partition Management**

```java
// MySQL
ALTER TABLE events ADD PARTITION (
    PARTITION p2025_09_21 VALUES LESS THAN (TO_DAYS('2025-09-22'))
);

// PostgreSQL
CREATE TABLE events_20250921 PARTITION OF events
FOR VALUES FROM ('2025-09-21') TO ('2025-09-22');

// Oracle
ALTER TABLE events ADD PARTITION p2025_09_21
VALUES LESS THAN (DATE '2025-09-22');

// SQL Server
ALTER PARTITION FUNCTION PF_events()
SPLIT RANGE ('2025-09-21');
```

## Factory Pattern

### SqlGeneratorFactory

```java
public class SqlGeneratorFactory {
    private static final Map<DatabaseType, SqlGenerator> cache =
        new ConcurrentHashMap<>();

    public static SqlGenerator getGenerator(DatabaseType type) {
        return cache.computeIfAbsent(type,
            SqlGeneratorFactory::createGenerator);
    }

    private static SqlGenerator createGenerator(DatabaseType type) {
        switch (type) {
            case MYSQL: return new MySQLSqlGenerator();
            case POSTGRESQL: return new PostgreSQLSqlGenerator();
            case ORACLE: return new OracleSqlGenerator();
            case SQLSERVER: return new SqlServerSqlGenerator();
            default: return new BaseSqlGenerator();
        }
    }

    public static DatabaseType detectDatabaseType(Connection conn)
            throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String productName = metaData.getDatabaseProductName().toLowerCase();

        if (productName.contains("mysql")) return DatabaseType.MYSQL;
        if (productName.contains("postgresql")) return DatabaseType.POSTGRESQL;
        if (productName.contains("oracle")) return DatabaseType.ORACLE;
        if (productName.contains("sql server")) return DatabaseType.SQLSERVER;

        return DatabaseType.GENERIC;
    }
}
```

## Performance Optimizations

### 1. **Prepared Statements**

```java
// Reuse prepared statements
private final Map<String, PreparedStatement> stmtCache = new HashMap<>();

public PreparedStatement getPreparedStatement(Connection conn, String sql)
        throws SQLException {
    return stmtCache.computeIfAbsent(sql, k -> {
        try {
            return conn.prepareStatement(k);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });
}
```

### 2. **Batch Processing**

```java
// Optimal batch sizes per database
public int getOptimalBatchSize(DatabaseType type) {
    switch (type) {
        case MYSQL: return 1000;      // MySQL handles large batches well
        case POSTGRESQL: return 500;  // PostgreSQL prefers smaller batches
        case ORACLE: return 100;      // Oracle has limitations on bind variables
        case SQLSERVER: return 200;   // SQL Server balanced approach
        default: return 100;
    }
}
```

### 3. **Index Hints**

```java
// MySQL index hints
SELECT * FROM table USE INDEX (idx_partition_col)
WHERE partition_col BETWEEN ? AND ?

// Oracle hints
SELECT /*+ INDEX(table idx_partition_col) */ *
FROM table WHERE partition_col BETWEEN ? AND ?

// PostgreSQL (via session settings)
SET enable_seqscan = off;
SELECT * FROM table WHERE partition_col BETWEEN ? AND ?;
```

## SQL Injection Prevention

### 1. **Parameter Binding**

```java
// Always use parameters, never concatenate
String sql = "SELECT * FROM table WHERE id = ?";
stmt.setString(1, userId);  // Safe

// Never do this
String sql = "SELECT * FROM table WHERE id = '" + userId + "'";  // Vulnerable
```

### 2. **Identifier Escaping**

```java
public String escapeIdentifier(String identifier) {
    // Remove any escape characters first
    identifier = identifier.replace("`", "")
                          .replace("\"", "")
                          .replace("[", "")
                          .replace("]", "");

    // Apply database-specific escaping
    if (this instanceof MySQLSqlGenerator) {
        return "`" + identifier + "`";
    } else if (this instanceof SqlServerSqlGenerator) {
        return "[" + identifier + "]";
    } else {
        return "\"" + identifier + "\"";  // ANSI standard
    }
}
```

### 3. **Whitelist Validation**

```java
private static final Set<String> VALID_COLUMNS = Set.of(
    "id", "created_at", "updated_at", "status"
);

public String validateColumn(String column) {
    if (!VALID_COLUMNS.contains(column.toLowerCase())) {
        throw new IllegalArgumentException("Invalid column: " + column);
    }
    return column;
}
```

## Future Enhancements

### 1. **Query Builder DSL**

```java
Query query = QueryBuilder.select()
    .from("events")
    .where("event_time").between(startDate, endDate)
    .and("status").equals("ACTIVE")
    .orderBy("event_time", DESC)
    .limit(100);

String sql = sqlGenerator.generate(query);
```

### 2. **Schema Migration Support**

```java
public interface SchemaManager {
    void createSchema(EntityMetadata metadata);
    void migrateSchema(EntityMetadata from, EntityMetadata to);
    void validateSchema(EntityMetadata metadata);
}
```

### 3. **Query Plan Analysis**

```java
public class QueryAnalyzer {
    public QueryPlan analyze(String sql, DatabaseType type) {
        // Execute EXPLAIN and parse results
        return new QueryPlan(estimatedCost, estimatedRows, indexUsage);
    }
}