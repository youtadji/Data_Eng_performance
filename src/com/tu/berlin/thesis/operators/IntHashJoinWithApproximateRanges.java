package com.tu.berlin.thesis.operators;

import com.tu.berlin.thesis.rangetree.ExactRangesIndex;
import com.tu.berlin.thesis.rangetree.RangeApproximator;
import com.tu.berlin.thesis.rangetree.RangeExtractor;

import java.util.*;

public class IntHashJoinWithApproximateRanges implements IntOperator {

    private final IntOperator leftOp;
    private final IntOperator rightOp;
    private final int leftKeyIndex;
    private final int rightKeyIndex;
    private final int clusterCount;
    private final int targetRangeCount;

    private final Map<Integer, List<int[]>> hashTable = new HashMap<>();
    private final RangeExtractor extractor;
    private final ExactRangesIndex ranges = new ExactRangesIndex();

    private int[] currentRightRow;
    private Iterator<int[]> matchIterator;

    private int rangePasses = 0;
    private int rangeRejects = 0;
    private int hashLookups = 0;
    private int actualMatches = 0;

    private int exactRangeCount = 0;
    private int groupedRangeCount = 0;
    private int approximateRangeCount = 0;

    // New constructor
    public IntHashJoinWithApproximateRanges(
            IntOperator left,
            IntOperator right,
            int leftKeyIndex,
            int rightKeyIndex,
            int expectedBuildKeys,
            int clusterCount,
            int targetRangeCount
    ) {
        this.leftOp = left;
        this.rightOp = right;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
        this.extractor = new RangeExtractor(expectedBuildKeys);
        this.clusterCount = clusterCount;
        this.targetRangeCount = targetRangeCount;
    }

    // Backward-compatible old constructor
    public IntHashJoinWithApproximateRanges(
            IntOperator left,
            IntOperator right,
            int leftKeyIndex,
            int rightKeyIndex,
            int expectedBuildKeys,
            int targetRangeCount
    ) {
        this(left, right, leftKeyIndex, rightKeyIndex, expectedBuildKeys, Integer.MAX_VALUE, targetRangeCount);
    }

    public int getRangePasses() { return rangePasses; }
    public int getRangeRejects() { return rangeRejects; }
    public int getHashLookups() { return hashLookups; }
    public int getActualMatches() { return actualMatches; }
    public int getExactRangeCount() { return exactRangeCount; }
    public int getGroupedRangeCount() { return groupedRangeCount; }
    public int getApproximateRangeCount() { return approximateRangeCount; }
    public int getRangeCount() { return ranges.getRangeCount(); }
    public long getRangeBytes() { return ranges.approxBytesUsed(); }

    @Override
    public void open() {
        System.out.println("IntHashJoin WITH ApproximateRanges: OPEN");

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

        RangeExtractor.Ranges exact = extractor.buildExactRanges();
        exactRangeCount = exact.count;

        RangeExtractor.Ranges grouped = RangeExtractor.regroupToTargetClusters(exact, clusterCount);
        groupedRangeCount = grouped.count;

        RangeExtractor.Ranges approx = RangeApproximator.approximate(grouped, targetRangeCount);
        approximateRangeCount = approx.count;

        ranges.build(approx.starts, approx.ends, approx.count);

        System.out.println("  Built hash table with " + leftCount +
                " rows (" + hashTable.size() + " distinct keys)");
        System.out.println("  Natural exact ranges=" + exactRangeCount +
                ", grouped ranges=" + groupedRangeCount +
                ", approximate ranges=" + approximateRangeCount +
                ", approxBytes=" + ranges.approxBytesUsed());

        rightOp.open();
        advanceToNextMatch();
    }

    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {
            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            int key = currentRightRow[rightKeyIndex];

            if (!ranges.contains(key)) {
                rangeRejects++;
                continue;
            }
            rangePasses++;

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
        System.out.println("IntHashJoin WITH ApproximateRanges: CLOSE");
    }
}