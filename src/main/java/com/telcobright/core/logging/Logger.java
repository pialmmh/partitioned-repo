package com.telcobright.core.logging;

import java.util.Map;

/**
 * Framework-agnostic logging interface for the partitioned repository library.
 * This allows integration with any logging framework (SLF4J, JUL, Quarkus, etc.)
 * 
 * Implementations should be thread-safe.
 */
public interface Logger {
    
    /**
     * Log level enumeration
     */
    enum Level {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR
    }
    
    /**
     * Check if a log level is enabled
     */
    boolean isLevelEnabled(Level level);
    
    /**
     * Log a simple message
     */
    void log(Level level, String message);
    
    /**
     * Log a message with exception
     */
    void log(Level level, String message, Throwable throwable);
    
    /**
     * Log a structured event with additional context
     * 
     * @param level Log level
     * @param eventType Type of event (e.g., "PARTITION_CREATED", "MAINTENANCE_STARTED")
     * @param message Human-readable message
     * @param context Additional structured data (key-value pairs)
     */
    void logEvent(Level level, String eventType, String message, Map<String, Object> context);
    
    // Convenience methods
    default void trace(String message) {
        log(Level.TRACE, message);
    }
    
    default void debug(String message) {
        log(Level.DEBUG, message);
    }
    
    default void info(String message) {
        log(Level.INFO, message);
    }
    
    default void warn(String message) {
        log(Level.WARN, message);
    }
    
    default void warn(String message, Throwable throwable) {
        log(Level.WARN, message, throwable);
    }
    
    default void error(String message) {
        log(Level.ERROR, message);
    }
    
    default void error(String message, Throwable throwable) {
        log(Level.ERROR, message, throwable);
    }
}