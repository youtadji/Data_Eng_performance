package com.tu.berlin.thesis.operators;

import java.util.*;
import java.util.BitSet;

public class IntHashJoinWithBloomFilter implements IntOperator {

    private final IntOperator leftOp;
    private final IntOperator rightOp;

    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Bloom bloom;
    private final Map<Integer, List<int[]>> hashTable = new HashMap<>();

    // probe state
    private int[] currentRightRow;
    private Iterator<int[]> matchIterator;

    // metrics
    private int bloomPasses = 0;
    private int bloomRejects = 0;
    private int hashLookups = 0;
    private int actualMatches = 0;

    // getters
    public int getBloomPasses()   { return bloomPasses; }
    public int getBloomRejects()  { return bloomRejects; }
    public int getHashLookups()   { return hashLookups; }
    public int getActualMatches() { return actualMatches; }

    public IntHashJoinWithBloomFilter(
            IntOperator left,
            IntOperator right,
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

    @Override
    public void open() {
        System.out.println("IntHashJoin WITH Bloom: OPEN");
        System.out.println("  Bloom(m=" + bloom.size + ", k=" + bloom.k + ")");

        // ------------------------
        // BUILD PHASE
        // ------------------------
        leftOp.open();
        int[] leftRow;
        int leftCount = 0;

        while ((leftRow = leftOp.next()) != null) {
            int key = leftRow[leftKeyIndex];

            // add key to Bloom
            bloom.add(key);

            // add row to hash table
            List<int[]> tmp = hashTable.get(key);
            if (tmp == null) {
                tmp = new ArrayList<>();
                hashTable.put(key, tmp);
            }
            tmp.add(leftRow);

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

    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            int key = currentRightRow[rightKeyIndex];

            // bloom test
            if (!bloom.mightContain(key)) {
                bloomRejects++;
                continue;
            }
            bloomPasses++;

            // hash lookup
            hashLookups++;
            List<int[]> matches = hashTable.get(key);

            if (matches != null) {
                matchIterator = matches.iterator();
                actualMatches += matches.size();
            }
        }
    }

    @Override
    public int[] next() {
        while (true) {
            if (currentRightRow == null) return null;

            if (matchIterator != null && matchIterator.hasNext()) {
                int[] leftRow = matchIterator.next();

                int[] out = new int[leftRow.length + currentRightRow.length];
                System.arraycopy(leftRow, 0, out, 0, leftRow.length);
                System.arraycopy(currentRightRow, 0, out, leftRow.length, currentRightRow.length);

                if (!matchIterator.hasNext()) advanceToNextMatch();
                return out;
            }

            advanceToNextMatch();
        }
    }

    @Override
    public void close() {
        rightOp.close();
        hashTable.clear();
        System.out.println("IntHashJoin WITH Bloom: CLOSE");
    }

    // ============================================================
    // INNER BLOOM FILTER CLASS (INT) — FAST (matches String style)
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

        void add(int key) {
            for (int i = 0; i < k; i++) {
                int h = computeHash(key, i);
                bits.set(h);
            }
        }

        boolean mightContain(int key) {
            for (int i = 0; i < k; i++) {
                int h = computeHash(key, i);
                if (!bits.get(h)) return false;
            }
            return true;
        }

        private int computeHash(int key, int i) {
            int hash = key;
            hash = hash * multipliers[i % multipliers.length];
            return (hash & 0x7fffffff) % size;
        }
         //positive mask , modula en negatuve would be problematic
        double estimateFalsePositiveRate(int numInsertedKeys) {
            double load = (double) numInsertedKeys / size;
            return Math.pow(1 - Math.exp(-k * load), k);
        }
    }
}
