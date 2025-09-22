# Supported Data Types in SplitVerse

## ⚠️ IMPORTANT: Restrictions Apply ONLY to @Id and @ShardingKey Fields

**Regular entity fields (without @Id or @ShardingKey annotations) can use ANY Java data type including Date, Timestamp, ZonedDateTime, or any other type. The library does NOT validate regular fields.**

### Quick Reference Table

| Field Type | Allowed Data Types | Restrictions |
|------------|-------------------|--------------|
| **@Id** | `String` only | MUST be String for consistent sharding |
| **@ShardingKey** | `LocalDateTime`, `String`, `Long`, `Integer` | NO Date, Timestamp, or other date types |
| **Regular Fields** | **ANY Java type** | No restrictions - use Date, Timestamp, anything |

**The data type restrictions documented here apply ONLY to:**
- Fields annotated with `@Id` (must be String)
- Fields annotated with `@ShardingKey` (must be LocalDateTime, String, Long, or Integer)

**Regular fields (without these annotations) can use ANY Java data type** including Date, Timestamp, ZonedDateTime, or any other type.

## Overview
SplitVerse enforces strict data type requirements **only for critical fields** (@Id and @ShardingKey) to ensure consistent behavior across all database systems and prevent timezone-related issues in partitioning and sharding operations.

## Date and Time Types (For @ShardingKey Fields Only)

### Supported for @ShardingKey
- **Java Type**: `java.time.LocalDateTime` (ONLY for date-based partitioning)
- **MySQL Type**: `DATETIME` (automatically used)
- **Behavior**: Stores date and time without timezone information

### NOT Supported for @ShardingKey
The following date/time types are explicitly rejected **for @ShardingKey fields** and will cause runtime errors:
- `java.util.Date` - Rejected due to timezone ambiguity
- `java.sql.Date` - Rejected, use LocalDateTime instead
- `java.sql.Timestamp` - Rejected due to timezone conversion issues
- `java.sql.Time` - Not supported
- `java.time.ZonedDateTime` - Not supported due to timezone complexity
- `java.time.OffsetDateTime` - Not supported
- `java.time.Instant` - Not supported

**Note**: These types CAN be used in regular entity fields, just not in @ShardingKey fields.

### Why LocalDateTime Only?
1. **Consistency**: LocalDateTime provides consistent behavior across all timezones
2. **No Conversion Issues**: Avoids MySQL JDBC driver timezone conversions
3. **Partition Compatibility**: Works seamlessly with date-based partitioning
4. **Precision**: Supports microsecond precision (MySQL limitation)

### Example Usage
```java
@Table(name = "events")
public class Event implements ShardingEntity<LocalDateTime> {
    @ShardingKey
    @Column(name = "created_at")
    private LocalDateTime createdAt;  // ✓ Correct

    // private Date createdAt;         // ✗ Will throw exception
    // private Timestamp createdAt;    // ✗ Will throw exception
}
```

## Other Supported Data Types

### Primitive Types
- `String` - Maps to VARCHAR/TEXT
- `Integer` / `int` - Maps to INT
- `Long` / `long` - Maps to BIGINT
- `Double` / `double` - Maps to DOUBLE
- `Float` / `float` - Maps to FLOAT
- `Boolean` / `boolean` - Maps to TINYINT(1)

### Other Types
- `BigDecimal` - Maps to DECIMAL (for precise numeric values)
- `byte[]` - Maps to BLOB (for binary data)

## Best Practices

### Working with LocalDateTime
```java
// Creating timestamps
LocalDateTime now = LocalDateTime.now();
LocalDateTime specific = LocalDateTime.of(2025, 9, 21, 10, 30, 0);

// Date arithmetic
LocalDateTime tomorrow = now.plusDays(1);
LocalDateTime lastWeek = now.minusWeeks(1);

// Truncating to specific precision
LocalDateTime dayStart = now.truncatedTo(ChronoUnit.DAYS);
LocalDateTime hourStart = now.truncatedTo(ChronoUnit.HOURS);
```

### Timezone Considerations
If your application needs to handle multiple timezones:
1. Store all times in UTC using LocalDateTime
2. Convert to user timezone only for display
3. Convert from user timezone to UTC before storing

```java
// Convert from ZonedDateTime to LocalDateTime (for storage)
ZonedDateTime userTime = ZonedDateTime.now(ZoneId.of("America/New_York"));
LocalDateTime utcTime = userTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();

// Convert from LocalDateTime to ZonedDateTime (for display)
LocalDateTime storedTime = entity.getCreatedAt();
ZonedDateTime displayTime = storedTime.atZone(ZoneOffset.UTC)
    .withZoneSameInstant(ZoneId.of("America/New_York"));
```

## Validation

### Critical Fields (Strictly Validated)
The library validates data types for these critical fields at initialization:

#### @Id Field
- **MUST be**: `String` (required for consistent hash-based sharding)
- Any other type will throw `IllegalArgumentException`

#### @ShardingKey Field
- **For date-based partitioning**: Must be `LocalDateTime`
- **For hash-based partitioning**: Can be `String`
- **For value-based partitioning**: Can be `Long` or `Integer`
- Unsupported date types (Date, Timestamp, ZonedDateTime, etc.) will throw exceptions

### Regular Fields (No Validation)
Regular entity fields (without @Id or @ShardingKey annotations) can use ANY data type, including:
- `java.util.Date` - Allowed in regular fields
- `java.sql.Timestamp` - Allowed in regular fields
- `ZonedDateTime` - Allowed in regular fields
- Any other Java type

### Example: Mixed Types
```java
@Table(name = "events")
public class Event implements ShardingEntity<LocalDateTime> {
    @Id
    private String id;                    // ✓ Must be String

    @ShardingKey
    private LocalDateTime createdAt;      // ✓ Must be LocalDateTime for date partitioning

    // Regular fields - ANY type allowed
    private java.util.Date legacyDate;    // ✓ OK - regular field
    private Timestamp auditTimestamp;     // ✓ OK - regular field
    private ZonedDateTime userTimezone;   // ✓ OK - regular field
    private String description;           // ✓ OK - regular field
}
```

### When Validation Occurs
- Validation happens during repository initialization
- Errors are caught early, not at runtime
- Clear error messages indicate which field and type caused the issue

## Migration Guide
If migrating from Date/Timestamp to LocalDateTime:

1. Update entity fields:
```java
// Before
private Date createdAt;

// After
private LocalDateTime createdAt;
```

2. Update database schema:
```sql
ALTER TABLE your_table
MODIFY COLUMN created_at DATETIME(6);
```

3. Convert existing data if needed:
```java
// Converting Date to LocalDateTime
Date oldDate = ...;
LocalDateTime newDateTime = oldDate.toInstant()
    .atZone(ZoneId.systemDefault())
    .toLocalDateTime();
```