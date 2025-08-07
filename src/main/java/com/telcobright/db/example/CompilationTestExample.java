package com.telcobright.db.example;

import com.telcobright.db.entity.SmsEntity;
import com.telcobright.db.entity.OrderEntity;
import com.telcobright.db.repository.GenericMultiTableRepository;
import com.telcobright.db.repository.GenericPartitionedTableRepository;

/**
 * Simple compilation test to verify all repositories can be instantiated
 */
public class CompilationTestExample {
    
    public static void main(String[] args) {
        System.out.println("=== Compilation Test ===");
        
        try {
            // Test Multi-Table Repository instantiation
            System.out.println("Creating GenericMultiTableRepository...");
            GenericMultiTableRepository<SmsEntity, Long> smsRepo = 
                GenericMultiTableRepository.<SmsEntity, Long>builder(SmsEntity.class, Long.class)
                    .database("test")
                    .username("root")
                    .password("123456")
                    .autoManagePartitions(false)  // Disable for test
                    .initializePartitionsOnStart(false) // Disable for test
                    .build();
            
            System.out.println(" GenericMultiTableRepository created successfully");
            
            // Test Partitioned Table Repository instantiation  
            System.out.println("Creating GenericPartitionedTableRepository...");
            GenericPartitionedTableRepository<OrderEntity, Long> orderRepo = 
                GenericPartitionedTableRepository.<OrderEntity, Long>builder(OrderEntity.class, Long.class)
                    .database("test")
                    .username("root")
                    .password("123456")
                    .autoManagePartitions(false)  // Disable for test
                    .initializePartitionsOnStart(false) // Disable for test
                    .build();
            
            System.out.println(" GenericPartitionedTableRepository created successfully");
            
            // Test shutdown
            System.out.println("Shutting down repositories...");
            smsRepo.shutdown();
            orderRepo.shutdown();
            
            System.out.println(" All repositories shut down successfully");
            System.out.println(" Compilation and instantiation test PASSED");
            
        } catch (Exception e) {
            System.err.println(" Test FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }
}