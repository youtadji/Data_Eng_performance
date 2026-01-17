package com.tu.berlin.thesis.operators;

import java.util.*;
import java.util.BitSet;

/**
 * Hash Join WITH configurable Bloom filter.
 *
 * Build Phase:
 *   - Insert left keys into Bloom filter
 *   - Insert left rows into hash table
 *
 * Probe Phase:
 *   - Check Bloom filter first (fast, avoids cache misses)
 *   - If Bloom says "might be present", then probe hash table
 *
 * Metrics collected:
 *   bloomPasses      = number of right rows that passed Bloom test
 *   bloomRejects     = number of right rows skipped by Bloom
 *   hashLookups      = number of actual hash table lookups performed
 *   actualMatches    = total matched left rows (for debugging)
 */
public class HashJoinWithBloomFilter implements Operator {

    private final Operator leftOp;
    private final Operator rightOp;

    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Bloom bloom;   // configurable bloom filter
    private final Map<String, List<String[]>> hashTable = new HashMap<>();

    // probe state
    private String[] currentRightRow;
    private Iterator<String[]> matchIterator;

    //  metrics
    private int bloomPasses = 0;
    private int bloomRejects = 0;
    private int hashLookups = 0;
    private int actualMatches = 0;

    // getters for metrics
    public int getBloomPasses()  { return bloomPasses; }
    public int getBloomRejects() { return bloomRejects; }
    public int getHashLookups()  { return hashLookups; }
    public int getActualMatches(){ return actualMatches; }

    public HashJoinWithBloomFilter(
            Operator left,
            Operator right,
            int leftKeyIndex,
            int rightKeyIndex,
            int filterSizeBits,
            int numHashFunctions
    ) {
        this.leftOp = left;
        this.rightOp = right;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
        this.bloom = new Bloom(filterSizeBits, numHashFunctions);
    }

    // ============================================================
    // OPEN = BUILD PHASE + prepare probe
    // ============================================================
    @Override
    public void open() {
        System.out.println("HashJoin WITH Bloom: OPEN");
        System.out.println("  Bloom(m=" + bloom.size + ", k=" + bloom.k + ")");

        // ------------------------
        // BUILD PHASE
        // ------------------------
        leftOp.open();
        String[] leftRow;
        int leftCount = 0;

        while ((leftRow = leftOp.next()) != null) {
            String key = leftRow[leftKeyIndex];

            // add to Bloom filter
            bloom.add(key);

            // add to hash table
            var tmp = hashTable.get(key); //one lookup
            if(tmp==null) {
                tmp= new ArrayList<>();
                hashTable.put(key,tmp);
            }
            tmp.add(leftRow);

            //hashTable.putIfAbsent(key, new ArrayList<>());
            //hashTable.get(key).add(leftRow);

            leftCount++;
        }
        leftOp.close();

        System.out.println("  Built hash table with " +
                leftCount + " rows (" + hashTable.size() + " distinct keys)");

        double fp = bloom.estimateFalsePositiveRate(leftCount);
        System.out.printf("  Estimated Bloom false-positive rate: %.3f%%%n", fp * 100);

        // prepare probe
        rightOp.open();
        advanceToNextMatch();
    }

    // ============================================================
    // Probe — find next matching row
    // ============================================================
    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            String key = currentRightRow[rightKeyIndex];

            // bloom test
            if (!bloom.mightContain(key)) {
                bloomRejects++;
                continue;  // skip, definitely not present
            }
            bloomPasses++;

            // Hash table lookup
            hashLookups++;
            List<String[]> matches = hashTable.get(key);

            if (matches != null) {
                matchIterator = matches.iterator();
                actualMatches += matches.size();
            }
        }
    }

    // ============================================================
    // next() = return joined row
    // ============================================================
    @Override
    public String[] next() {
        if (currentRightRow == null) return null;

        if (matchIterator != null && matchIterator.hasNext()) {
            String[] leftRow = matchIterator.next();

            // merge left+right rows
            String[] out = new String[leftRow.length + currentRightRow.length];
            System.arraycopy(leftRow, 0, out, 0, leftRow.length);
            System.arraycopy(currentRightRow, 0, out, leftRow.length, currentRightRow.length);

            if (!matchIterator.hasNext())
                advanceToNextMatch();

            return out;
        }

        advanceToNextMatch();
        return next();
    }

    // ============================================================
    // close()
    // ============================================================
    @Override
    public void close() {
        rightOp.close();
        hashTable.clear();
        System.out.println("HashJoin WITH Bloom: CLOSE");
    }


    // ============================================================
    // INNER BLOOM FILTER CLASS
    // ============================================================
    private static class Bloom {
        static int[] multipliers = {31, 37, 41, 43, 47, 53, 59, 61, 67, 71};

        private final BitSet bits;
        private final int size;
        private final int k;

        Bloom(int sizeBits, int numHashFunctions) {
            this.size = sizeBits;
            this.k = numHashFunctions;
            this.bits = new BitSet(sizeBits);
        }

        void add(String key) {
            for (int i = 0; i < k; i++) {
                int h = computeHash(key, i);
                bits.set(h);
            }
        }

        boolean mightContain(String key) {
            for (int i = 0; i < k; i++) {
                int h = computeHash(key, i);
                if (!bits.get(h)) return false;
            }
            return true;
        }

        private int computeHash(String key, int i) {
            int hash = key.hashCode();
            hash = hash * multipliers[i % multipliers.length];
            return Math.abs(hash) % size;
        }

        double estimateFalsePositiveRate(int numInsertedKeys) {
            double load = (double) numInsertedKeys / size;
            return Math.pow(1 - Math.exp(-k * load), k);
        }
    }
}
