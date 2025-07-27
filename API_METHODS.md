# 📋 ShardingRepository API Methods

## ✅ **Standard CRUD Operations**

### Insert
```java
repo.insert(entity)                    // Auto-routes to correct table based on date
```

### Query Operations
```java
repo.findByDateRange(start, end)       // Efficient: Uses date for table targeting
repo.findByField("fieldName", value)  // Scans all tables (no date filtering)
repo.query("status = ?", "SENT")      // Custom WHERE clause across all tables
repo.count(start, end)                // Count across date range
```

## 🆔 **ID-Based Lookups** ⚠️

### findById() - Full Scan
```java
SmsEntity sms = repo.findById(123L);
```
**Performance Impact:**
- ⚠️ **Scans ALL tables** in retention window (e.g., 14 tables for 7-day retention)
- ⚠️ **No partition pruning** possible (ID doesn't contain date info)
- ⚠️ **Can be slow** for large retention windows
- ✅ **Use when you have no date context**

### findByIdAndDateRange() - Optimized Scan ⭐
```java
SmsEntity sms = repo.findByIdAndDateRange(123L, 
    LocalDateTime.now().minusDays(1),  // start date
    LocalDateTime.now().plusDays(1)    // end date
);
```
**Performance Benefits:**
- ✅ **Limited scope** - only scans tables in date range
- ✅ **Much faster** for narrow date ranges  
- ✅ **Recommended approach** when you know approximate date
- ✅ **Example**: 2-day range = only 2 tables scanned vs 14

## 📊 **Performance Comparison**

| Method | Tables Scanned | Use Case | Performance |
|--------|---------------|----------|-------------|
| `findByDateRange()` | Date-targeted | Known date range | ⚡ **Fast** |
| `findByField()` | All retention tables | Field-based search | ⚠️ **Slow** |
| `findByIdAndDateRange()` | Date range only | ID + date context | ✅ **Good** |
| `findById()` | All retention tables | ID only, no date | ⚠️ **Slowest** |

## 💡 **Best Practices**

### ✅ **Recommended Patterns**
```java
// Best: Use date-based queries when possible
repo.findByDateRange(yesterday, today);

// Good: Limit ID searches to narrow date ranges  
repo.findByIdAndDateRange(id, recentStart, recentEnd);

// Acceptable: Field queries with business logic to limit scope
repo.findByField("phoneNumber", "+123456789");
```

### ⚠️ **Use With Caution**
```java
// Avoid: Full retention window ID lookup
repo.findById(123L);  // May scan 14+ tables

// Consider: Add date context when possible
LocalDateTime approximateDate = getApproximateDateFromBusinessLogic();
repo.findByIdAndDateRange(123L, 
    approximateDate.minusDays(1), 
    approximateDate.plusDays(1)
);
```

## 🎯 **When to Use Each Method**

### **findById()** - Use when:
- ✅ You have no date context at all
- ✅ Performance is not critical
- ✅ Retention window is small (< 7 days)

### **findByIdAndDateRange()** - Use when:
- ⭐ **You know approximate date** (recommended)
- ⭐ Performance matters
- ⭐ Working with recent data (last few days)
- ⭐ You can derive date from business context

### **findByDateRange()** - Use when:
- ⚡ **You have date range** (most efficient)
- ⚡ Doing time-based analytics
- ⚡ Working with recent activity

---

*Choose the right method based on your data access patterns and performance requirements!* 🚀