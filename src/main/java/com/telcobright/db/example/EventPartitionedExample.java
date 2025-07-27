package com.telcobright.db.example;

import com.telcobright.db.ShardingRepositoryBuilder;
import com.telcobright.db.repository.ShardingRepository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Event Partitioned Table Example
 * 
 * Demonstrates:
 * - PARTITIONED_TABLE mode (single table with date-based partitions)
 * - Simplified builder pattern with type selection
 * - 30-day retention policy for event data
 * - Daily maintenance scheduler at 02:00
 * - Cross-partition querying and aggregation
 */
public class EventPartitionedExample {
    
    public static void main(String[] args) {
        try {
            System.out.println("=== Event Partitioned Table Sharding Example ===\n");
            
            // Create Event repository with partitioned table mode
            System.out.println("1. Creating Event repository with partitioned table mode...");
            System.out.println("   • Repository type: PARTITIONED_TABLE");
            System.out.println("   • Single logical table: event");
            System.out.println("   • Physical partitions: event_20250727, event_20250728, etc.");
            System.out.println("   • 30-day retention policy");
            System.out.println("   • Daily maintenance at 02:00");
            
            ShardingRepository<EventEntity> eventRepo = ShardingRepositoryBuilder
                .partitionedTable()              // Repository type: PARTITIONED_TABLE
                .host("127.0.0.1")
                .port(3306)
                .database("test")
                .username("root")
                .password("123456")
                .maxPoolSize(15)
                .buildRepository(EventEntity.class);
            
            System.out.println("   ✓ Partitioned repository ready with auto-management enabled\n");
            
            // Demo operations
            demonstrateEventOperations(eventRepo);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void demonstrateEventOperations(ShardingRepository<EventEntity> repository) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        
        // Insert event data across different days
        System.out.println("2. Inserting event data...");
        System.out.println("   • Events will be partitioned by timestamp");
        
        repository.insert(new EventEntity("LOGIN", "user123", now, "{\"ip\":\"192.168.1.1\"}", "INFO"));
        repository.insert(new EventEntity("ORDER", "user456", now, "{\"amount\":99.99}", "INFO"));
        repository.insert(new EventEntity("ERROR", "user789", now.minusDays(1), "{\"error\":\"timeout\"}", "ERROR"));
        repository.insert(new EventEntity("LOGOUT", "user123", now.minusDays(2), "{\"duration\":3600}", "INFO"));
        repository.insert(new EventEntity("PAYMENT", "user456", now.minusDays(3), "{\"method\":\"card\"}", "INFO"));
        
        System.out.println("   ✓ 5 events inserted across multiple partitions\n");
        
        // Query across partitions
        System.out.println("3. Cross-partition querying...");
        List<EventEntity> events = repository.findByDateRange(now.minusDays(30), now.plusDays(1));
        System.out.println("   ✓ Found " + events.size() + " events across all partitions:");
        events.forEach(event -> System.out.println("     - " + event.getEventType() + " by " + event.getUserId() + " at " + event.getTimestamp().toLocalDate()));
        System.out.println();
        
        // Count across partitions
        System.out.println("4. Cross-partition counting...");
        long count = repository.count(now.minusDays(30), now.plusDays(1));
        System.out.println("   ✓ Total events in retention window: " + count + "\n");
        
        // Field-based queries
        System.out.println("5. Field-based queries across partitions...");
        List<EventEntity> userEvents = repository.findByField("userId", "user123");
        System.out.println("   ✓ Events for user123: " + userEvents.size());
        
        List<EventEntity> errorEvents = repository.query("severity = ?", "ERROR");
        System.out.println("   ✓ ERROR events: " + errorEvents.size());
        
        List<EventEntity> loginEvents = repository.query("event_type = ?", "LOGIN");
        System.out.println("   ✓ LOGIN events: " + loginEvents.size() + "\n");
        
        // Find by ID operations
        System.out.println("6. Find by ID operations across partitions...");
        
        if (!events.isEmpty()) {
            Long sampleId = events.get(0).getId();
            
            // Find by ID across all partitions
            System.out.println("   • Finding by ID across all partitions...");
            EventEntity foundById = repository.findById(sampleId);
            if (foundById != null) {
                System.out.println("   ✓ Found event by ID: " + foundById.getEventType() + " (" + foundById.getSeverity() + ")");
            } else {
                System.out.println("   ⚠ Event not found by ID");
            }
            
            // Find by ID with date range (optimized)
            System.out.println("   • Finding by ID with date range (optimized)...");
            EventEntity foundByIdAndDate = repository.findByIdAndDateRange(sampleId, now.minusDays(1), now.plusDays(1));
            if (foundByIdAndDate != null) {
                System.out.println("   ✓ Found event by ID+date: " + foundByIdAndDate.getEventType() + " (" + foundByIdAndDate.getSeverity() + ")");
            } else {
                System.out.println("   ⚠ Event not found in specified date range");
            }
        } else {
            System.out.println("   ⚠ No events available for ID lookup test");
        }
        System.out.println();
        
        // Show partitioned table features
        System.out.println("7. Partitioned table features:");
        System.out.println("   ✓ Single logical table: event");
        System.out.println("   ✓ Date-based partitions: event_YYYYMMDD");
        System.out.println("   ✓ 30-day retention policy");
        System.out.println("   ✓ Daily scheduler at 02:00 (partition management)");
        System.out.println("   ✓ Cross-partition queries and aggregations");
        System.out.println("   ✓ Optimized partition pruning for date ranges");
        System.out.println();
        
        System.out.println("🎉 Event Partitioned Table Sharding Complete!");
        System.out.println("   ✓ PARTITIONED_TABLE mode (single table + partitions)");
        System.out.println("   ✓ Auto partition creation and management");
        System.out.println("   ✓ 30-day retention with automatic cleanup");
        System.out.println("   ✓ Cross-partition queries work seamlessly");
        System.out.println("   ✓ Find by ID with partition scanning");
        System.out.println("   ✓ Zero-boilerplate repository usage");
    }
}