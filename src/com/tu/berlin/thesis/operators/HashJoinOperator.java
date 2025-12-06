package com.tu.berlin.thesis.operators;

import java.util.*;

/**
 * Clean Hash Join WITHOUT any Bloom filter.
 *
 * Build Phase:
 *   - Insert left rows into hash table
 *   Read all rows from the left input, build a hash table
 *
 * Probe Phase:
 *   - For each right row, directly probe the hash table
 *   -for each right row, use join key, lookup in hash table, output matches.
 * Metrics:
 *   hashLookups      = number of hash table accesses
 *   actualMatches    = number of matched rows (for debugging)
 */
public class HashJoinOperator implements Operator {

    private final Operator leftOp;
    private final Operator rightOp;

    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Map<String, List<String[]>> hashTable = new HashMap<>();

    private String[] currentRightRow;
    private Iterator<String[]> matchIterator;

    // === Metrics ===
    private int hashLookups = 0;
    private int actualMatches = 0;

    public int getHashLookups()   { return hashLookups; }
    public int getActualMatches() { return actualMatches; }


    public HashJoinOperator(
            Operator left,
            Operator right,
            int leftKeyIndex,
            int rightKeyIndex
    ) {
        this.leftOp = left;
        this.rightOp = right;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
    }

    // ============================================================
    // OPEN = BUILD PHASE + prepare for probe
    // ============================================================
    @Override
    public void open() {
        System.out.println("HashJoin WITHOUT Bloom: OPEN");

        // -----------------------
        // BUILD PHASE
        // -----------------------

        //read left side into hash table
        leftOp.open();
        String[] leftRow;
        int leftCount = 0;

        while ((leftRow = leftOp.next()) != null) {
            String key = leftRow[leftKeyIndex];

            hashTable.putIfAbsent(key, new ArrayList<>());
            hashTable.get(key).add(leftRow);

            leftCount++;
        }
        leftOp.close();

        System.out.println("  Built hash table with "
                + leftCount + " rows (" + hashTable.size() + " distinct keys)");

        // Prepare probe
        //open right side and move to first match
        rightOp.open();
        advanceToNextMatch();
    }


    // ============================================================
    // PROBE — find next right row with matches
    // ============================================================
    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            String key = currentRightRow[rightKeyIndex];

            // DIRECT hash lookup (no Bloom)
            hashLookups++;
            List<String[]> matches = hashTable.get(key);

            if (matches != null) {
                matchIterator = matches.iterator();
                actualMatches += matches.size();
            }
        }
    }


    // ============================================================
    // NEXT = return next joined row
    // ============================================================
    @Override
    public String[] next() {
        if (currentRightRow == null) return null;

        if (matchIterator != null && matchIterator.hasNext()) {
            String[] leftRow = matchIterator.next();

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
    // CLOSE
    // ============================================================
    @Override
    public void close() {
        rightOp.close();
        hashTable.clear();
        System.out.println("HashJoin WITHOUT Bloom: CLOSE");
    }
}
