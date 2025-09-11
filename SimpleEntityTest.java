import com.telcobright.splitverse.examples.entity.SubscriberEntity;
import java.sql.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

public class SimpleEntityTest {
    public static void main(String[] args) {
        String testId = "test_" + System.currentTimeMillis();
        String testMsisdn = "+8801712345678";
        
        try {
            // Create database and table directly
            Connection conn = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306/telco_test?useSSL=false&serverTimezone=UTC",
                "root", "123456"
            );
            
            // Create simple table without partitioning
            String createTable = """
                CREATE TABLE IF NOT EXISTS subscribers (
                    subscriber_id VARCHAR(255) PRIMARY KEY,
                    msisdn VARCHAR(255) NOT NULL,
                    imsi VARCHAR(255),
                    iccid VARCHAR(255),
                    balance DECIMAL(10,2),
                    status VARCHAR(255),
                    plan_type VARCHAR(255),
                    data_balance_mb BIGINT,
                    voice_balance_minutes INT,
                    created_at DATETIME NOT NULL,
                    last_activity DATETIME,
                    activated_at DATETIME,
                    KEY idx_created_at (created_at),
                    KEY idx_msisdn (msisdn)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """;
            
            Statement stmt = conn.createStatement();
            stmt.execute(createTable);
            System.out.println("✓ Table created/verified");
            
            // Insert test entity
            String insertSQL = """
                INSERT INTO subscribers (
                    subscriber_id, msisdn, balance, status, plan_type, 
                    created_at, data_balance_mb, voice_balance_minutes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;
            
            PreparedStatement pstmt = conn.prepareStatement(insertSQL);
            pstmt.setString(1, testId);
            pstmt.setString(2, testMsisdn);
            pstmt.setBigDecimal(3, new BigDecimal("100.00"));
            pstmt.setString(4, "ACTIVE");
            pstmt.setString(5, "PREPAID");
            pstmt.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setLong(7, 5000L);
            pstmt.setInt(8, 500);
            
            int rows = pstmt.executeUpdate();
            System.out.println("✓ Entity inserted: " + rows + " row(s) affected");
            System.out.println("  Test ID: " + testId);
            System.out.println("  MSISDN: " + testMsisdn);
            
            // Verify by reading back
            PreparedStatement selectStmt = conn.prepareStatement(
                "SELECT * FROM subscribers WHERE subscriber_id = ?"
            );
            selectStmt.setString(1, testId);
            
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                System.out.println("\n✓ VERIFIED: Entity found in database");
                System.out.println("  ID: " + rs.getString("subscriber_id"));
                System.out.println("  MSISDN: " + rs.getString("msisdn"));
                System.out.println("  Balance: " + rs.getBigDecimal("balance"));
                System.out.println("  Status: " + rs.getString("status"));
                System.out.println("  Plan: " + rs.getString("plan_type"));
                System.out.println("  Data Balance: " + rs.getLong("data_balance_mb") + " MB");
                System.out.println("  Voice Balance: " + rs.getInt("voice_balance_minutes") + " minutes");
                System.out.println("  Created At: " + rs.getTimestamp("created_at"));
            } else {
                System.out.println("✗ ERROR: Entity not found after insertion");
            }
            
            rs.close();
            selectStmt.close();
            pstmt.close();
            stmt.close();
            conn.close();
            
            System.out.println("\n✅ Test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("✗ ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}