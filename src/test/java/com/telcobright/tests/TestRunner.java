package com.telcobright.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Main test runner that executes all test suites
 */
public class TestRunner {
    
    public static void main(String[] args) {
        System.out.println("==========================================================");
        System.out.println("    SHARDING REPOSITORY TEST SUITE");
        System.out.println("==========================================================");
        System.out.println("Start Time: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        System.out.println();
        
        boolean allTestsPassed = true;
        
        // Run Multi-Table Repository Tests
        System.out.println("----------------------------------------------------------");
        System.out.println("SUITE 1: MULTI-TABLE REPOSITORY TESTS");
        System.out.println("----------------------------------------------------------");
        try {
            MultiTableRepositoryTest multiTableTest = new MultiTableRepositoryTest();
            multiTableTest.runAllTests();
            multiTableTest.printTestSummary();
            System.out.println("✅ Multi-Table Repository Tests PASSED");
        } catch (Exception e) {
            System.err.println("❌ Multi-Table Repository Tests FAILED: " + e.getMessage());
            e.printStackTrace();
            allTestsPassed = false;
        }
        
        System.out.println();
        
        // Run Partitioned Table Repository Tests
        System.out.println("----------------------------------------------------------");
        System.out.println("SUITE 2: PARTITIONED TABLE REPOSITORY TESTS");
        System.out.println("----------------------------------------------------------");
        try {
            PartitionedTableRepositoryTest partitionedTest = new PartitionedTableRepositoryTest();
            partitionedTest.runAllTests();
            partitionedTest.printTestSummary();
            System.out.println("✅ Partitioned Table Repository Tests PASSED");
        } catch (Exception e) {
            System.err.println("❌ Partitioned Table Repository Tests FAILED: " + e.getMessage());
            e.printStackTrace();
            allTestsPassed = false;
        }
        
        System.out.println();
        
        // Run Performance and Stress Tests
        System.out.println("----------------------------------------------------------");
        System.out.println("SUITE 3: PERFORMANCE AND STRESS TESTS");
        System.out.println("----------------------------------------------------------");
        try {
            PerformanceStressTest perfTest = new PerformanceStressTest();
            perfTest.runAllTests();
            perfTest.printTestSummary();
            System.out.println("✅ Performance and Stress Tests COMPLETED");
        } catch (Exception e) {
            System.err.println("❌ Performance and Stress Tests FAILED: " + e.getMessage());
            e.printStackTrace();
            allTestsPassed = false;
        }
        
        System.out.println();
        
        // Run Data Integrity Tests
        System.out.println("----------------------------------------------------------");
        System.out.println("SUITE 4: DATA INTEGRITY TESTS");
        System.out.println("----------------------------------------------------------");
        try {
            DataIntegrityTest integrityTest = new DataIntegrityTest();
            integrityTest.runAllTests();
            integrityTest.printTestSummary();
            System.out.println("✅ Data Integrity Tests PASSED");
        } catch (Exception e) {
            System.err.println("❌ Data Integrity Tests FAILED: " + e.getMessage());
            e.printStackTrace();
            allTestsPassed = false;
        }
        
        System.out.println();
        
        // Run Concurrent Operations Tests
        System.out.println("----------------------------------------------------------");
        System.out.println("SUITE 5: CONCURRENT OPERATIONS TESTS");
        System.out.println("----------------------------------------------------------");
        try {
            ConcurrentOperationsTest concurrentTest = new ConcurrentOperationsTest();
            concurrentTest.runAllTests();
            concurrentTest.printTestSummary();
            System.out.println("✅ Concurrent Operations Tests PASSED");
        } catch (Exception e) {
            System.err.println("❌ Concurrent Operations Tests FAILED: " + e.getMessage());
            e.printStackTrace();
            allTestsPassed = false;
        }
        
        System.out.println();
        System.out.println("==========================================================");
        System.out.println("    FINAL TEST SUMMARY");
        System.out.println("==========================================================");
        
        // Generate final summary from database
        generateFinalSummary();
        
        System.out.println();
        System.out.println("End Time: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        
        if (allTestsPassed) {
            System.out.println();
            System.out.println("🎉 ALL TEST SUITES PASSED SUCCESSFULLY! 🎉");
            System.out.println("==========================================================");
            System.exit(0);
        } else {
            System.out.println();
            System.out.println("⚠️  SOME TESTS FAILED - REVIEW LOGS ABOVE ⚠️");
            System.out.println("==========================================================");
            System.exit(1);
        }
    }
    
    private static void generateFinalSummary() {
        try {
            Connection conn = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306/test", "root", "123456"
            );
            
            Statement stmt = conn.createStatement();
            
            // Get test results summary
            ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "  COUNT(*) as total_tests, " +
                "  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed, " +
                "  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed, " +
                "  SUM(total_operations) as total_ops, " +
                "  AVG(avg_latency_ms) as avg_latency, " +
                "  MAX(p99_latency_ms) as max_p99_latency " +
                "FROM test_results"
            );
            
            if (rs.next()) {
                System.out.println("Test Execution Statistics:");
                System.out.println("  Total Tests Run: " + rs.getInt("total_tests"));
                System.out.println("  Tests Passed: " + rs.getInt("passed"));
                System.out.println("  Tests Failed: " + rs.getInt("failed"));
                System.out.println("  Total Operations: " + rs.getLong("total_ops"));
                System.out.println("  Average Latency: " + 
                    String.format("%.2f ms", rs.getDouble("avg_latency")));
                System.out.println("  Max P99 Latency: " + 
                    String.format("%.2f ms", rs.getDouble("max_p99_latency")));
            }
            
            // Get performance metrics
            rs = stmt.executeQuery(
                "SELECT metric_name, AVG(metric_value) as avg_value " +
                "FROM test_metrics " +
                "WHERE metric_name LIKE '%TPS%' OR metric_name LIKE '%throughput%' " +
                "GROUP BY metric_name " +
                "ORDER BY avg_value DESC " +
                "LIMIT 5"
            );
            
            System.out.println("\nTop Performance Metrics:");
            while (rs.next()) {
                System.out.println("  " + rs.getString("metric_name") + ": " + 
                    String.format("%.0f", rs.getDouble("avg_value")));
            }
            
            // Get table statistics
            rs = stmt.executeQuery(
                "SELECT " +
                "  COUNT(*) as table_count, " +
                "  SUM(table_rows) as total_rows, " +
                "  SUM(data_length + index_length)/1024/1024 as total_size_mb " +
                "FROM information_schema.tables " +
                "WHERE table_schema = 'test' " +
                "AND (table_name LIKE 'perf_%' OR table_name LIKE 'sms_%' OR table_name LIKE 'orders%')"
            );
            
            if (rs.next()) {
                System.out.println("\nDatabase Statistics:");
                System.out.println("  Tables Created: " + rs.getInt("table_count"));
                System.out.println("  Total Rows: " + rs.getLong("total_rows"));
                System.out.println("  Database Size: " + 
                    String.format("%.2f MB", rs.getDouble("total_size_mb")));
            }
            
            stmt.close();
            conn.close();
            
        } catch (Exception e) {
            System.err.println("Failed to generate summary: " + e.getMessage());
        }
    }
}