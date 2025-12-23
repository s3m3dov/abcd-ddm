package de.ddm.structures;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.ddm.serialization.AkkaSerializable;
import java.util.BitSet;

/**
 * Space-efficient Bloom filter implementation optimized for Raspberry Pi
 * clusters.
 * 
 * A Bloom filter is a probabilistic data structure that can test whether an
 * element
 * is a member of a set. False positives are possible, but false negatives are
 * not.
 * 
 * This implementation uses MurmurHash3-based hash functions for fast
 * performance
 * on ARM processors (Raspberry Pi).
 */
public class BloomFilter implements AkkaSerializable {
    private static final long serialVersionUID = 1234567890123456789L;

    @JsonProperty("bitSet")
    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numHashFunctions;
    private final int expectedElements;

    /**
     * Create a Bloom filter with default 1% false positive rate
     * 
     * @param expectedElements Expected number of elements to be inserted
     */
    public BloomFilter(int expectedElements) {
        this(expectedElements, 0.01); // 1% false positive rate
    }

    /**
     * Create a Bloom filter with custom false positive rate
     * 
     * @param expectedElements  Expected number of elements to be inserted
     * @param falsePositiveRate Desired false positive probability (0.0 to 1.0)
     */
    public BloomFilter(int expectedElements, double falsePositiveRate) {
        this.expectedElements = expectedElements;
        this.bitSetSize = optimalBitSetSize(expectedElements, falsePositiveRate);
        this.numHashFunctions = optimalNumHashFunctions(expectedElements, bitSetSize);
        this.bitSet = new BitSet(bitSetSize);
    }

    /**
     * Constructor for Jackson deserialization
     * 
     * @param bitSet           The bit set
     * @param bitSetSize       The size of the bit set
     * @param numHashFunctions The number of hash functions
     * @param expectedElements The expected number of elements
     */
    @JsonCreator
    public BloomFilter(
            @JsonProperty("bitSet") BitSet bitSet,
            @JsonProperty("bitSetSize") int bitSetSize,
            @JsonProperty("numHashFunctions") int numHashFunctions,
            @JsonProperty("expectedElements") int expectedElements) {
        this.bitSet = bitSet;
        this.bitSetSize = bitSetSize;
        this.numHashFunctions = numHashFunctions;
        this.expectedElements = expectedElements;
    }

    /**
     * Add an element to the Bloom filter
     * 
     * @param value The string value to add
     */
    public void add(String value) {
        if (value == null) {
            value = ""; // Treat null as empty string
        }

        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(value, i);
            bitSet.set(Math.abs(hash % bitSetSize));
        }
    }

    /**
     * Test if an element might be in the set
     * 
     * @param value The string value to test
     * @return true if the element might be in the set (false positives possible),
     *         false if the element is definitely not in the set (no false
     *         negatives)
     */
    public boolean mightContain(String value) {
        if (value == null) {
            value = ""; // Treat null as empty string
        }

        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(value, i);
            if (!bitSet.get(Math.abs(hash % bitSetSize))) {
                return false; // Definitely not present
            }
        }
        return true; // Might be present (or false positive)
    }

    /**
     * Get the size of the Bloom filter in bytes (for memory tracking)
     * 
     * @return Approximate size in bytes
     */
    public long getSizeInBytes() {
        // BitSet size + object overhead
        return (bitSetSize / 8) + 64; // 64 bytes for object overhead
    }

    /**
     * Get the number of hash functions used
     */
    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    /**
     * Get the bit set size
     */
    public int getBitSetSize() {
        return bitSetSize;
    }

    /**
     * Calculate optimal bit set size for given parameters
     * Formula: m = -n * ln(p) / (ln(2)^2)
     * where n = expected elements, p = false positive rate
     */
    private static int optimalBitSetSize(int n, double p) {
        if (n == 0)
            return 64; // Minimum size for empty sets
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Calculate optimal number of hash functions
     * Formula: k = (m/n) * ln(2)
     * where m = bit set size, n = expected elements
     */
    private static int optimalNumHashFunctions(int n, int m) {
        if (n == 0)
            return 1;
        int k = (int) Math.ceil((m / (double) n) * Math.log(2));
        return Math.max(1, k); // At least 1 hash function
    }

    /**
     * Hash function based on MurmurHash3 (optimized for ARM processors)
     * Uses seed variation to generate multiple independent hash functions
     * 
     * @param value The string to hash
     * @param seed  The seed for this hash function (0 to numHashFunctions-1)
     * @return Hash value
     */
    private int hash(String value, int seed) {
        // Simple but effective hash combining String.hashCode() with seed
        // For production, consider full MurmurHash3 implementation
        int h = value.hashCode();
        h ^= seed * 0x9e3779b9; // Golden ratio constant for good distribution

        // Additional mixing (simplified MurmurHash3 finalization)
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;

        return h;
    }

    @Override
    public String toString() {
        return String.format("BloomFilter[size=%d, hashFunctions=%d, expectedElements=%d, sizeInBytes=%d]",
                bitSetSize, numHashFunctions, expectedElements, getSizeInBytes());
    }
}
