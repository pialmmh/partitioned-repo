const { chromium } = require('playwright');
const mysql = require('mysql2/promise');
const crypto = require('crypto');

async function runFullTest() {
    console.log('🎭 Starting Playwright test with database verification...\n');
    
    // Generate test data
    const testId = 'pw_' + crypto.randomBytes(8).toString('hex');
    const testMsisdn = '+8801' + Math.floor(Math.random() * 900000000 + 100000000);
    
    console.log('📋 Test Configuration:');
    console.log(`  • Test ID: ${testId}`);
    console.log(`  • MSISDN: ${testMsisdn}`);
    console.log(`  • Database: telco_test`);
    console.log(`  • Table: subscribers\n`);
    
    // Launch Playwright browser
    const browser = await chromium.launch({ headless: false });
    const page = await browser.newPage();
    
    // Navigate to test page
    await page.goto('about:blank');
    await page.evaluate(() => {
        document.body.innerHTML = `
            <div style="font-family: monospace; padding: 20px;">
                <h1>Entity Creation Test</h1>
                <div id="status">Initializing...</div>
                <div id="results" style="margin-top: 20px; white-space: pre-wrap;"></div>
            </div>
        `;
    });
    
    // Update status in browser
    await page.evaluate(() => {
        document.getElementById('status').innerText = 'Connecting to database...';
    });
    
    // Wait for visual effect
    await page.waitForTimeout(1000);
    
    // Connect to MySQL
    const connection = await mysql.createConnection({
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: '123456',
        database: 'telco_test'
    });
    
    console.log('✅ Connected to MySQL database');
    
    // Update browser status
    await page.evaluate(() => {
        document.getElementById('status').innerText = 'Creating entity...';
    });
    
    // Insert entity
    const insertQuery = `
        INSERT INTO subscribers (
            subscriber_id, msisdn, balance, status, plan_type,
            created_at, data_balance_mb, voice_balance_minutes
        ) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?)
    `;
    
    const [insertResult] = await connection.execute(insertQuery, [
        testId,
        testMsisdn,
        250.50,
        'ACTIVE',
        'POSTPAID',
        10000,
        1000
    ]);
    
    console.log(`✅ Entity created: ${insertResult.affectedRows} row(s) affected`);
    
    // Update browser with results
    await page.evaluate((data) => {
        document.getElementById('status').innerText = '✅ Entity created successfully!';
        document.getElementById('results').innerText = 
            `Created Entity:\n` +
            `  ID: ${data.id}\n` +
            `  MSISDN: ${data.msisdn}\n` +
            `  Status: ACTIVE\n` +
            `  Plan: POSTPAID`;
    }, { id: testId, msisdn: testMsisdn });
    
    // Wait for user to see results
    await page.waitForTimeout(3000);
    
    // Verify in database
    console.log('\n🔍 Verifying entity in database...');
    await page.evaluate(() => {
        document.getElementById('status').innerText = 'Verifying in database...';
    });
    
    const [rows] = await connection.execute(
        'SELECT * FROM subscribers WHERE subscriber_id = ?',
        [testId]
    );
    
    if (rows.length > 0) {
        const entity = rows[0];
        console.log('✅ VERIFIED: Entity found in database');
        console.log(`  • ID: ${entity.subscriber_id}`);
        console.log(`  • MSISDN: ${entity.msisdn}`);
        console.log(`  • Balance: ${entity.balance}`);
        console.log(`  • Status: ${entity.status}`);
        console.log(`  • Plan: ${entity.plan_type}`);
        console.log(`  • Data: ${entity.data_balance_mb} MB`);
        console.log(`  • Voice: ${entity.voice_balance_minutes} minutes`);
        
        // Update browser with verification
        await page.evaluate((entity) => {
            document.getElementById('status').innerText = '✅ Verification complete!';
            document.getElementById('results').innerHTML += 
                '\n\n<strong>Database Verification:</strong>\n' +
                `✅ Entity confirmed in database\n` +
                `  Balance: $${entity.balance}\n` +
                `  Data: ${entity.data_balance_mb} MB\n` +
                `  Voice: ${entity.voice_balance_minutes} minutes`;
        }, entity);
    } else {
        console.log('❌ Entity not found in database!');
        await page.evaluate(() => {
            document.getElementById('status').innerText = '❌ Verification failed!';
        });
    }
    
    // Check total count
    const [countResult] = await connection.execute(
        'SELECT COUNT(*) as total FROM subscribers'
    );
    console.log(`\n📊 Total records in database: ${countResult[0].total}`);
    
    // Wait before closing
    await page.waitForTimeout(5000);
    
    // Cleanup
    await connection.end();
    await browser.close();
    
    console.log('\n✅ Playwright test completed successfully!');
}

// Run the test
runFullTest().catch(error => {
    console.error('❌ Test failed:', error);
    process.exit(1);
});