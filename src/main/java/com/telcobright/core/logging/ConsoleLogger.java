package com.telcobright.core.logging;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Default console-based logger implementation.
 * Thread-safe implementation that outputs to System.out/System.err.
 */
public class ConsoleLogger implements Logger {
    
    private static final DateTimeFormatter TIMESTAMP_FORMAT = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    private final String name;
    private final Level minimumLevel;
    
    /**
     * Create a console logger with default INFO level
     */
    public ConsoleLogger(String name) {
        this(name, Level.INFO);
    }
    
    /**
     * Create a console logger with specified minimum level
     */
    public ConsoleLogger(String name, Level minimumLevel) {
        this.name = name;
        this.minimumLevel = minimumLevel;
    }
    
    @Override
    public boolean isLevelEnabled(Level level) {
        return level.ordinal() >= minimumLevel.ordinal();
    }
    
    @Override
    public void log(Level level, String message) {
        if (!isLevelEnabled(level)) {
            return;
        }
        
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        String output = String.format("[%s] %-5s [%s] %s", 
            timestamp, level, name, message);
        
        if (level == Level.ERROR || level == Level.WARN) {
            System.err.println(output);
        } else {
            System.out.println(output);
        }
    }
    
    @Override
    public void log(Level level, String message, Throwable throwable) {
        if (!isLevelEnabled(level)) {
            return;
        }
        
        log(level, message);
        if (throwable != null) {
            throwable.printStackTrace(level == Level.ERROR || level == Level.WARN ? 
                System.err : System.out);
        }
    }
    
    @Override
    public void logEvent(Level level, String eventType, String message, Map<String, Object> context) {
        if (!isLevelEnabled(level)) {
            return;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(message);
        sb.append(" [event=").append(eventType);
        
        if (context != null && !context.isEmpty()) {
            context.forEach((key, value) -> 
                sb.append(", ").append(key).append("=").append(value));
        }
        sb.append("]");
        
        log(level, sb.toString());
    }
}