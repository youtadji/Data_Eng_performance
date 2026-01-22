package com.tu.berlin.thesis.operators;

import com.tu.berlin.thesis.rangetree.ExactRangesIndex;
import com.tu.berlin.thesis.rangetree.RangeExtractor;

import java.util.*;

public class IntHashJoinWithExactRanges implements IntOperator {

    private final IntOperator leftOp;
    private final IntOperator rightOp;
    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Map<Integer, List<int[]>> hashTable = new HashMap<>();

    // exact ranges (2 arrays)
    private final RangeExtractor extractor;
    private final ExactRangesIndex ranges = new ExactRangesIndex();

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
    public int getRangeCount() { return ranges.getRangeCount(); }
    public long getRangeBytes() { return ranges.approxBytesUsed(); }

    public IntHashJoinWithExactRanges(
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
        System.out.println("IntHashJoin WITH ExactRanges: OPEN");

        // ------------------------
        // BUILD
        // ------------------------
        leftOp.open();
        int[] leftRow;
        int leftCount = 0;

        while ((leftRow = leftOp.next()) != null) {
            int key = leftRow[leftKeyIndex];

            extractor.add(key);

            List<int[]> tmp = hashTable.get(key);
            if (tmp == null) {
                tmp = new ArrayList<>();
                hashTable.put(key, tmp);
            }
            tmp.add(leftRow);

            leftCount++;
        }
        leftOp.close();

        // build exact ranges from keys
        RangeExtractor.Ranges r = extractor.buildExactRanges();
        ranges.build(r.starts, r.ends, r.count);

        System.out.println("  Built hash table with " + leftCount +
                " rows (" + hashTable.size() + " distinct keys)");
        System.out.println("  ExactRanges ranges=" + ranges.getRangeCount() +
                ", approxBytes=" + ranges.approxBytesUsed());

        // ------------------------
        // PROBE
        // ------------------------
        rightOp.open();
        advanceToNextMatch();
    }

    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            int key = currentRightRow[rightKeyIndex];

            // exact ranges prefilter
            if (!ranges.contains(key)) {
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
        System.out.println("IntHashJoin WITH ExactRanges: CLOSE");
    }
}
