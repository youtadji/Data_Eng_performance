package com.tu.berlin.thesis.operators;

import com.tu.berlin.thesis.rangetree.RangeBPlusTree;
import com.tu.berlin.thesis.rangetree.RangeExtractor;

import java.util.*;

public class IntHashJoinWithRangeTree implements IntOperator {

    private final IntOperator leftOp;
    private final IntOperator rightOp;
    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Map<Integer, List<int[]>> hashTable = new HashMap<>();

    // range tree components
    private final RangeExtractor extractor;
    private final RangeBPlusTree rangeTree = new RangeBPlusTree();

    // probe state
    private int[] currentRightRow;
    private Iterator<int[]> matchIterator;

    // metrics
    private int rangePasses = 0;
    private int rangeRejects = 0;
    private int hashLookups = 0;
    private int actualMatches = 0;

    public int getRangePasses() { return rangePasses; }
    public int getRangeRejects() { return rangeRejects; }
    public int getHashLookups() { return hashLookups; }
    public int getActualMatches() { return actualMatches; }
    public int getRangeCount() { return rangeTree.getRangeCount(); }
    public long getRangeBytes() { return rangeTree.approxBytesUsed(); }

    public IntHashJoinWithRangeTree(
            IntOperator left,
            IntOperator right,
            int leftKeyIndex,
            int rightKeyIndex,
            int expectedBuildKeys
    ) {
        this.leftOp = left;
        this.rightOp = right;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
        this.extractor = new RangeExtractor(expectedBuildKeys);
    }

    @Override
    public void open() {
        System.out.println("IntHashJoin WITH RangeTree: OPEN");

        // ------------------------
        // BUILD PHASE
        // ------------------------
        leftOp.open();
        int[] leftRow;
        int leftCount = 0;

        while ((leftRow = leftOp.next()) != null) {
            int key = leftRow[leftKeyIndex];

            // collect for range extraction
            extractor.add(key);

            // build hash table (manual tmp, no compute if Aabsent)
            List<int[]> tmp = hashTable.get(key);
            if (tmp == null) {
                tmp = new ArrayList<>();
                hashTable.put(key, tmp);
            }
            tmp.add(leftRow);

            leftCount++;
        }
        leftOp.close();

        // build exact ranges + build B+Tree
        RangeExtractor.Ranges ranges = extractor.buildExactRanges();
        rangeTree.buildFromRanges(ranges.starts, ranges.ends, ranges.count);

        System.out.println("  Built hash table with " + leftCount +
                " rows (" + hashTable.size() + " distinct keys)");
        System.out.println("  RangeTree ranges=" + rangeTree.getRangeCount() +
                ", approxBytes=" + rangeTree.approxBytesUsed());

        // ------------------------
        // PROBE PREPARE
        // ------------------------
        rightOp.open();
        advanceToNextMatch();
    }

    // ============================================================
    // Probe — find next right row that has matches
    // ============================================================
    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            int key = currentRightRow[rightKeyIndex];

            // range prefilter
            if (!rangeTree.contains(key)) {
                rangeRejects++;
                continue;
            }
            rangePasses++;

            // hash lookup
            hashLookups++;
            List<int[]> matches = hashTable.get(key);

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
    public int[] next() {
        while (true) {
            if (currentRightRow == null) return null;

            if (matchIterator != null && matchIterator.hasNext()) {
                int[] leftRow = matchIterator.next();

                int[] out = new int[leftRow.length + currentRightRow.length];
                System.arraycopy(leftRow, 0, out, 0, leftRow.length);
                System.arraycopy(currentRightRow, 0, out, leftRow.length, currentRightRow.length);

                if (!matchIterator.hasNext()) {
                    advanceToNextMatch();
                }
                return out;
            }

            advanceToNextMatch();
        }
    }

    // ============================================================
    // close()
    // ============================================================
    @Override
    public void close() {
        rightOp.close();
        hashTable.clear();
        System.out.println("IntHashJoin WITH RangeTree: CLOSE");
    }
}
