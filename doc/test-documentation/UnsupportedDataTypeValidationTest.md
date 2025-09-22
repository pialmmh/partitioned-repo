# Unsupported Data Type Validation Test

## Test Objective
Verify that the SplitVerse repository properly validates and rejects entities that use unsupported date/time types in critical fields (@Id and @ShardingKey), while allowing any data types in regular fields.

## Test Scope

### Critical Fields (Must Validate)
1. **@ShardingKey field** - Must be one of:
   - `LocalDateTime` (for date-based partitioning)
   - `String` (for hash-based partitioning)
   - `Long` (for value-based partitioning)
   - `Integer` (for value-based partitioning)

2. **@Id field** - Must be:
   - `String` (required for consistent hash-based sharding)

### Regular Fields (No Validation)
Regular entity fields (without @Id or @ShardingKey annotations) can use any data type and should NOT trigger validation errors.

## Test Cases

### Test 1: Entity with java.util.Date as ShardingKey
```java
@Table(name = "test_entity")
public class DateShardingEntity {
    @Id private String id;
    @ShardingKey private java.util.Date createdDate;  // ❌ Should throw exception
    private String name;  // Regular field - OK
}
```
**Expected**: IllegalArgumentException during repository initialization
**Message**: Should indicate that only LocalDateTime is supported for date/time sharding keys

### Test 2: Entity with java.sql.Timestamp as ShardingKey
```java
@Table(name = "test_entity")
public class TimestampShardingEntity {
    @Id private String id;
    @ShardingKey private java.sql.Timestamp createdAt;  // ❌ Should throw exception
    private String description;
}
```
**Expected**: IllegalArgumentException during repository initialization
**Message**: Should reference doc/supported-data-types.md

### Test 3: Entity with ZonedDateTime as ShardingKey
```java
@Table(name = "test_entity")
public class ZonedDateTimeShardingEntity {
    @Id private String id;
    @ShardingKey private ZonedDateTime zonedTime;  // ❌ Should throw exception
    private Integer count;
}
```
**Expected**: IllegalArgumentException during repository initialization

### Test 4: Entity with Instant as ShardingKey
```java
@Table(name = "test_entity")
public class InstantShardingEntity {
    @Id private String id;
    @ShardingKey private Instant instantTime;  // ❌ Should throw exception
    private BigDecimal amount;
}
```
**Expected**: IllegalArgumentException during repository initialization

### Test 5: Entity with Long as Id (Invalid ID Type)
```java
@Table(name = "test_entity")
public class LongIdEntity {
    @Id private Long id;  // ❌ Should throw exception (must be String)
    @ShardingKey private LocalDateTime createdAt;
    private String content;
}
```
**Expected**: IllegalArgumentException during repository initialization
**Message**: Should indicate that @Id field must be String type

### Test 6: Entity with Integer as Id (Invalid ID Type)
```java
@Table(name = "test_entity")
public class IntegerIdEntity {
    @Id private Integer id;  // ❌ Should throw exception (must be String)
    @ShardingKey private LocalDateTime createdAt;
    private String data;
}
```
**Expected**: IllegalArgumentException during repository initialization

### Test 7: Valid Entity with LocalDateTime ShardingKey
```java
@Table(name = "test_entity")
public class ValidLocalDateTimeEntity {
    @Id private String id;  // ✓ Valid
    @ShardingKey private LocalDateTime createdAt;  // ✓ Valid
    private java.util.Date legacyDate;  // ✓ OK in regular field
    private Timestamp someTimestamp;  // ✓ OK in regular field
    private String name;
}
```
**Expected**: Repository initializes successfully (no exception)
**Note**: Regular fields can have any date type - validation only applies to @ShardingKey

### Test 8: Valid Entity with String ShardingKey
```java
@Table(name = "test_entity")
public class ValidStringShardingEntity {
    @Id private String id;  // ✓ Valid
    @ShardingKey private String category;  // ✓ Valid for hash-based
    private Date someDate;  // ✓ OK in regular field
    private String description;
}
```
**Expected**: Repository initializes successfully

### Test 9: Valid Entity with Long ShardingKey
```java
@Table(name = "test_entity")
public class ValidLongShardingEntity {
    @Id private String id;  // ✓ Valid
    @ShardingKey private Long sequence;  // ✓ Valid for value-based
    private Timestamp timestamp;  // ✓ OK in regular field
}
```
**Expected**: Repository initializes successfully

### Test 10: Entity with Multiple Date Fields (Mixed Types)
```java
@Table(name = "test_entity")
public class MixedDateFieldEntity {
    @Id private String id;  // ✓ Valid
    @ShardingKey private LocalDateTime createdAt;  // ✓ Valid
    private java.util.Date lastModified;  // ✓ OK - regular field
    private java.sql.Timestamp deletedAt;  // ✓ OK - regular field
    private ZonedDateTime scheduledTime;  // ✓ OK - regular field
    private Instant processedAt;  // ✓ OK - regular field
}
```
**Expected**: Repository initializes successfully
**Note**: Demonstrates that only @ShardingKey field is validated for date types

## Test Implementation Strategy

1. **Test Structure**:
   - Create a test class `UnsupportedDataTypeValidationTest`
   - Each test case attempts to build a repository with a specific entity type
   - Use `assertThrows` to verify that exceptions are thrown for invalid types
   - Verify exception messages contain helpful information

2. **Exception Verification**:
   - Check exception type is `IllegalArgumentException`
   - Verify message contains field name
   - Verify message contains the unsupported type name
   - Verify message references documentation when applicable

3. **Success Cases**:
   - Build repository without exception for valid entities
   - Optionally perform a simple insert/retrieve to confirm functionality

## Expected Benefits

1. **Early Detection**: Errors are caught at repository initialization, not runtime
2. **Clear Error Messages**: Developers get specific guidance on what's wrong
3. **Documentation Reference**: Error messages point to supported-data-types.md
4. **Flexibility**: Regular fields can use any types, only critical fields are restricted

## Implementation Notes

- All validation happens in `EntityMetadata` constructor
- Validation runs once during repository initialization
- No performance impact on runtime operations
- Clear separation between critical fields (@Id, @ShardingKey) and regular fields