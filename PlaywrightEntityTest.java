
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
            subscriber.setId("64135ab0fb758e9bbcaf315ee1f0851e");
            subscriber.setMsisdn("+8801258556750");
            subscriber.setBalance(new BigDecimal("100.00"));
            subscriber.setStatus("ACTIVE");
            subscriber.setPlan("PREPAID");
            subscriber.setCreatedAt(LocalDateTime.now());
            subscriber.setDataBalanceMb(5000L);
            subscriber.setVoiceBalanceMinutes(500);
            
            // Insert entity
            repo.insert(subscriber);
            
            System.out.println("SUCCESS: Entity created with ID: 64135ab0fb758e9bbcaf315ee1f0851e");
            
            // Verify by reading back
            SubscriberEntity found = repo.findById("64135ab0fb758e9bbcaf315ee1f0851e");
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
