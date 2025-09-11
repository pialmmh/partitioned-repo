package com.telcobright.splitverse.routing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hash-based routing for shard selection.
 * Uses MD5 for simplicity, can be replaced with MurmurHash3 for better performance.
 */
public class HashRouter {
    
    private final int totalShards;
    
    public HashRouter(int totalShards) {
        if (totalShards <= 0) {
            throw new IllegalArgumentException("Total shards must be positive");
        }
        this.totalShards = totalShards;
    }
    
    /**
     * Get shard index for a given key using hash-based routing.
     * 
     * @param key The sharding key (user ID, phone number, etc.)
     * @return Shard index (0 to totalShards-1)
     */
    public int getShardIndex(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        
        String keyString = String.valueOf(key);
        long hash = computeHash(keyString);
        
        // Use modulo to get shard index
        return (int) (Math.abs(hash) % totalShards);
    }
    
    /**
     * Get shard ID for a given key and shard IDs array.
     */
    public String getShardId(Object key, String[] shardIds) {
        if (shardIds == null || shardIds.length == 0) {
            throw new IllegalArgumentException("Shard IDs array cannot be empty");
        }
        
        int index = getShardIndex(key);
        // Handle case where we have fewer actual shards than total configured
        index = index % shardIds.length;
        return shardIds[index];
    }
    
    private long computeHash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(key.getBytes(StandardCharsets.UTF_8));
            
            // Convert first 8 bytes to long
            long hash = 0;
            for (int i = 0; i < Math.min(8, hashBytes.length); i++) {
                hash = (hash << 8) | (hashBytes[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash code
            return key.hashCode();
        }
    }
    
    /**
     * Check if a key would be routed to a specific shard.
     * Useful for debugging and testing.
     */
    public boolean isKeyInShard(Object key, int shardIndex) {
        return getShardIndex(key) == shardIndex;
    }
    
    /**
     * Get distribution statistics for a set of keys.
     * Useful for testing hash distribution.
     */
    public int[] getDistribution(Object[] keys) {
        int[] distribution = new int[totalShards];
        for (Object key : keys) {
            int shardIndex = getShardIndex(key);
            distribution[shardIndex]++;
        }
        return distribution;
    }
}