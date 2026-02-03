package org.lsm;

import java.util.BitSet;
import java.util.Objects;

public class BloomFilter {
    private final BitSet bitSet;
    private final int size;
    private final int numHashes;

    public BloomFilter(int numEntries) {
        // Rule of thumb: 10 bits per entry gives ~1% false positive rate
        this.size = numEntries * 10;
        this.bitSet = new BitSet(size);
        this.numHashes = 3; // We'll use 3 different hash perspectives
    }

    public void add(String key) {
        for (int i = 0; i < numHashes; i++) {
            int hash = computeHash(key, i);
            bitSet.set(Math.abs(hash % size));
        }
    }

    public boolean mightContain(String key) {
        for (int i = 0; i < numHashes; i++) {
            int hash = computeHash(key, i);
            if (!bitSet.get(Math.abs(hash % size))) {
                return false; // Definitely not present
            }
        }
        return true; // Might be present
    }

    private int computeHash(String key, int seed) {
        // Simple but effective: mix the key with a seed to get different hashes
        return Objects.hash(key, seed, "LSM-SALT");
    }
}