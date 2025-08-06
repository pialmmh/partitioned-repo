package com.telcobright.db.monitoring;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Simple HTTP server for exposing Prometheus metrics
 * Uses built-in Java HTTP server - no external dependencies required
 */
public class SimpleHttpMetricsServer {
    
    private final HttpServer server;
    private final MonitoringService monitoringService;
    private final int port;
    private final String path;
    
    public SimpleHttpMetricsServer(MonitoringService monitoringService, int port, String path) throws IOException {
        this.monitoringService = monitoringService;
        this.port = port;
        this.path = path;
        
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(path, new MetricsHandler());
        server.setExecutor(Executors.newFixedThreadPool(2)); // Small thread pool for metrics requests
    }
    
    public void start() {
        server.start();
        System.out.printf("Prometheus metrics server started on port %d, endpoint: http://localhost:%d%s%n", 
                         port, port, path);
    }
    
    public void stop() {
        server.stop(0);
        System.out.printf("Prometheus metrics server stopped%n");
    }
    
    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    String metrics = monitoringService.getPrometheusMetrics();
                    
                    exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                    exchange.sendResponseHeaders(200, metrics.getBytes(StandardCharsets.UTF_8).length);
                    
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(metrics.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    String error = String.format("Error generating metrics: %s", e.getMessage());
                    exchange.sendResponseHeaders(500, error.getBytes(StandardCharsets.UTF_8).length);
                    
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(error.getBytes(StandardCharsets.UTF_8));
                    }
                }
            } else {
                // Method not allowed
                exchange.sendResponseHeaders(405, 0);
                exchange.getResponseBody().close();
            }
        }
    }
    
    /**
     * Create and start a metrics server for the given monitoring service
     */
    public static SimpleHttpMetricsServer startFor(MonitoringService monitoringService) throws IOException {
        MonitoringConfig config = monitoringService.getConfig();
        String path = config.getHttpEndpointPath();
        int port = extractPortFromPath(path, 8080); // Default port 8080
        
        SimpleHttpMetricsServer server = new SimpleHttpMetricsServer(monitoringService, port, path);
        server.start();
        return server;
    }
    
    private static int extractPortFromPath(String path, int defaultPort) {
        // Simple path parsing - for production use, implement proper URL parsing
        return defaultPort;
    }
}