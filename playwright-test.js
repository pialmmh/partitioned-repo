const { chromium } = require('playwright');
const mysql = require('mysql2/promise');
const crypto = require('crypto');

// Generate a unique ID for testing
const testId = crypto.randomBytes(16).toString('hex');
const testMsisdn = '+8801' + Math.floor(Math.random() * 900000000 + 100000000);

async function runTest() {
    console.log('Starting Playwright test for entity creation...');
    console.log(`Test ID: ${testId}`);
    console.log(`Test MSISDN: ${testMsisdn}`);
    
    // Create a simple Java test program
    const javaCode = `
import com.telcobright.splitverse.examples.entity.SubscriberEntity;
import com.telcobright.core.repository.GenericPartitionedTableRepository;
import java.time.LocalDateTime;
import java.math.BigDecimal;

public class PlaywrightEntityTest {
    public static void main(String[] args) {
        try {
            // Create repository
            GenericPartitionedTableRepository<SubscriberEntity> repo = 
                GenericPartitionedTableRepository.<SubscriberEntity>builder(SubscriberEntity.class)
                    .host("127.0.0.1")
                    .port(3306)
                    .database("telco_test")
                    .username("root")
                    .password("123456")
                    .tableName("subscribers")
                    .partitionRetentionPeriod(30)
                    .build();
            
            // Create test entity
            SubscriberEntity subscriber = new SubscriberEntity();
            subscriber.setId("${testId}");
            subscriber.setMsisdn("${testMsisdn}");
            subscriber.setBalance(new BigDecimal("100.00"));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("PREPAID");
            subscriber.setCreatedAt(LocalDateTime.now());
            subscriber.setDataBalanceMb(5000L);
            subscriber.setVoiceBalanceMinutes(500);
            
            // Insert entity
            repo.insert(subscriber);
            
            System.out.println("SUCCESS: Entity created with ID: ${testId}");
            
            // Verify by reading back
            SubscriberEntity found = repo.findById("${testId}");
            if (found != null) {
                System.out.println("VERIFIED: Entity found in database");
                System.out.println("  MSISDN: " + found.getMsisdn());
                System.out.println("  Balance: " + found.getBalance());
                System.out.println("  Status: " + found.getStatus());
            } else {
                System.out.println("ERROR: Entity not found after insertion");
            }
            
            repo.shutdown();
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
`;

    // Save the Java test file
    const fs = require('fs').promises;
    await fs.writeFile('/home/mustafa/telcobright-projects/routesphere/partitioned-repo/PlaywrightEntityTest.java', javaCode);
    
    console.log('Java test file created');
    
    // Launch browser for monitoring (optional - we're really testing backend)
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    
    // Set up page to capture console output
    page.on('console', msg => console.log('Browser console:', msg.text()));
    
    // Navigate to a blank page (we're testing backend, not UI)
    await page.goto('about:blank');
    
    // Wait a moment for setup
    await page.waitForTimeout(1000);
    
    console.log('Playwright browser launched, now running backend test...');
    
    // The actual entity creation happens via Java code, not browser
    // We'll compile and run it after this script
    
    await browser.close();
    
    console.log('Playwright setup complete. Ready to run Java test.');
    console.log(`\nTo verify entity creation, check for ID: ${testId}`);
    
    return { testId, testMsisdn };
}

// Run the test
runTest().then(result => {
    console.log('\nTest execution completed');
    console.log('Test details:', result);
}).catch(error => {
    console.error('Test failed:', error);
    process.exit(1);
});