package com.telcobright.core.monitoring;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for repository monitoring system.
 * 
 * Supports multiple delivery methods:
 * - Prometheus HTTP endpoint
 * - REST API push
 * - Kafka push (future)
 * - SLF4J logging
 */
public class MonitoringConfig {
    
    public enum DeliveryMethod {
        HTTP_ENDPOINT,    // Expose /metrics endpoint
        REST_PUSH,        // Push to REST endpoint
        KAFKA_PUSH,       // Push to Kafka (future)
        SLF4J_LOGGING,    // Log metrics via SLF4J
        CONSOLE_ONLY      // Console output only (for examples/testing)
    }
    
    public enum MetricFormat {
        PROMETHEUS,       // Prometheus format for HTTP endpoint
        JSON,            // JSON format for REST push
        FORMATTED_TEXT   // Human-readable text format for console
    }
    
    private final boolean enabled;
    private final long reportingIntervalSeconds;
    private final DeliveryMethod deliveryMethod;
    private final MetricFormat metricFormat;
    private final String httpEndpointPath;
    private final String restPushUrl;
    private final String kafkaTopicName;
    private final String serviceName;
    private final String instanceId;
    private final boolean includeSystemMetrics;
    private final boolean includeConnectionPoolMetrics;
    private final boolean includeQueryMetrics;
    private final boolean enableSlowQueryLogging;
    private final long slowQueryThresholdMs;
    
    private MonitoringConfig(Builder builder) {
        this.enabled = builder.enabled;
        this.reportingIntervalSeconds = builder.reportingIntervalSeconds;
        this.deliveryMethod = builder.deliveryMethod;
        this.metricFormat = builder.metricFormat;
        this.httpEndpointPath = builder.httpEndpointPath;
        this.restPushUrl = builder.restPushUrl;
        this.kafkaTopicName = builder.kafkaTopicName;
        this.serviceName = builder.serviceName;
        this.instanceId = builder.instanceId;
        this.includeSystemMetrics = builder.includeSystemMetrics;
        this.includeConnectionPoolMetrics = builder.includeConnectionPoolMetrics;
        this.includeQueryMetrics = builder.includeQueryMetrics;
        this.enableSlowQueryLogging = builder.enableSlowQueryLogging;
        this.slowQueryThresholdMs = builder.slowQueryThresholdMs;
    }
    
    // Getters
    public boolean isEnabled() { return enabled; }
    public long getReportingIntervalSeconds() { return reportingIntervalSeconds; }
    public DeliveryMethod getDeliveryMethod() { return deliveryMethod; }
    public MetricFormat getMetricFormat() { return metricFormat; }
    public String getHttpEndpointPath() { return httpEndpointPath; }
    public String getRestPushUrl() { return restPushUrl; }
    public String getKafkaTopicName() { return kafkaTopicName; }
    public String getServiceName() { return serviceName; }
    public String getInstanceId() { return instanceId; }
    public boolean isIncludeSystemMetrics() { return includeSystemMetrics; }
    public boolean isIncludeConnectionPoolMetrics() { return includeConnectionPoolMetrics; }
    public boolean isIncludeQueryMetrics() { return includeQueryMetrics; }
    public boolean isEnableSlowQueryLogging() { return enableSlowQueryLogging; }
    public long getSlowQueryThresholdMs() { return slowQueryThresholdMs; }
    
    /**
     * Creates a disabled monitoring configuration
     */
    public static MonitoringConfig disabled() {
        return new Builder().enabled(false).build();
    }
    
    /**
     * Creates a configuration for Prometheus HTTP endpoint
     */
    public static MonitoringConfig prometheusHttp(String endpointPath) {
        return new Builder()
                .enabled(true)
                .deliveryMethod(DeliveryMethod.HTTP_ENDPOINT)
                .metricFormat(MetricFormat.PROMETHEUS)
                .httpEndpointPath(endpointPath)
                .build();
    }
    
    /**
     * Creates a configuration for REST push
     */
    public static MonitoringConfig restPush(String pushUrl) {
        return new Builder()
                .enabled(true)
                .deliveryMethod(DeliveryMethod.REST_PUSH)
                .metricFormat(MetricFormat.JSON)
                .restPushUrl(pushUrl)
                .build();
    }
    
    /**
     * Creates a configuration for SLF4J logging
     */
    public static MonitoringConfig slf4jLogging() {
        return new Builder()
                .enabled(true)
                .deliveryMethod(DeliveryMethod.SLF4J_LOGGING)
                .metricFormat(MetricFormat.JSON)
                .build();
    }
    
    public static class Builder {
        private boolean enabled = true;
        private long reportingIntervalSeconds = 60; // Default 1 minute
        private DeliveryMethod deliveryMethod = DeliveryMethod.HTTP_ENDPOINT;
        private MetricFormat metricFormat = MetricFormat.PROMETHEUS;
        private String httpEndpointPath = "/metrics";
        private String restPushUrl = null;
        private String kafkaTopicName = "repository-metrics";
        private String serviceName = "partitioned-repo";
        private String instanceId = getDefaultInstanceId();
        private boolean includeSystemMetrics = false;
        private boolean includeConnectionPoolMetrics = false;
        private boolean includeQueryMetrics = false;
        private boolean enableSlowQueryLogging = false;
        private long slowQueryThresholdMs = 1000; // Default 1 second
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder reportingInterval(long interval, TimeUnit unit) {
            this.reportingIntervalSeconds = unit.toSeconds(interval);
            return this;
        }
        
        public Builder reportingIntervalSeconds(long seconds) {
            this.reportingIntervalSeconds = seconds;
            return this;
        }
        
        public Builder deliveryMethod(DeliveryMethod method) {
            this.deliveryMethod = method;
            return this;
        }
        
        public Builder metricFormat(MetricFormat format) {
            this.metricFormat = format;
            return this;
        }
        
        public Builder httpEndpointPath(String path) {
            this.httpEndpointPath = path;
            return this;
        }
        
        public Builder restPushUrl(String url) {
            this.restPushUrl = url;
            return this;
        }
        
        public Builder kafkaTopicName(String topicName) {
            this.kafkaTopicName = topicName;
            return this;
        }
        
        public Builder serviceName(String name) {
            this.serviceName = name;
            return this;
        }
        
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }
        
        public Builder includeSystemMetrics(boolean include) {
            this.includeSystemMetrics = include;
            return this;
        }
        
        public Builder includeConnectionPoolMetrics(boolean include) {
            this.includeConnectionPoolMetrics = include;
            return this;
        }
        
        public Builder includeQueryMetrics(boolean include) {
            this.includeQueryMetrics = include;
            return this;
        }
        
        public Builder enableSlowQueryLogging(boolean enable) {
            this.enableSlowQueryLogging = enable;
            return this;
        }
        
        public Builder slowQueryThresholdMs(long thresholdMs) {
            this.slowQueryThresholdMs = thresholdMs;
            return this;
        }
        
        public MonitoringConfig build() {
            validate();
            return new MonitoringConfig(this);
        }
        
        private void validate() {
            if (enabled) {
                if (reportingIntervalSeconds <= 0) {
                    throw new IllegalArgumentException("Reporting interval must be positive");
                }
                
                switch (deliveryMethod) {
                    case REST_PUSH:
                        if (restPushUrl == null || restPushUrl.trim().isEmpty()) {
                            throw new IllegalArgumentException("REST push URL is required for REST_PUSH delivery method");
                        }
                        break;
                    case HTTP_ENDPOINT:
                        if (httpEndpointPath == null || httpEndpointPath.trim().isEmpty()) {
                            throw new IllegalArgumentException("HTTP endpoint path is required for HTTP_ENDPOINT delivery method");
                        }
                        break;
                    case KAFKA_PUSH:
                        if (kafkaTopicName == null || kafkaTopicName.trim().isEmpty()) {
                            throw new IllegalArgumentException("Kafka topic name is required for KAFKA_PUSH delivery method");
                        }
                        break;
                }
                
                if (serviceName == null || serviceName.trim().isEmpty()) {
                    throw new IllegalArgumentException("Service name cannot be null or empty");
                }
            }
        }
        
        private static String getDefaultInstanceId() {
            try {
                return java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return "unknown-" + System.currentTimeMillis();
            }
        }
    }
    
    @Override
    public String toString() {
        return String.format(
            "MonitoringConfig{enabled=%s, interval=%ds, delivery=%s, format=%s, service=%s, instance=%s}",
            enabled, reportingIntervalSeconds, deliveryMethod, metricFormat, serviceName, instanceId
        );
    }
}