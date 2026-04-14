package com.tu.berlin.thesis.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Greedy streaming approximate range set.
 *
 * Keeps at most targetRangeCount ranges at any time.
 * When a new key causes the range count to exceed the target,
 * it greedily merges the adjacent pair with the smallest gap.
 *
 * This is memory-bounded and fully online.
 */
public class IntStreamingApproximateRangeSet {

    private final int targetRangeCount;
    private final TreeMap<Integer, Range> rangesByStart = new TreeMap<>();

    public IntStreamingApproximateRangeSet(int targetRangeCount) {
        if (targetRangeCount <= 0) {
            throw new IllegalArgumentException("targetRangeCount must be > 0");
        }
        this.targetRangeCount = targetRangeCount;
    }

    public void insert(int x) {
        Map.Entry<Integer, Range> leftEntry = rangesByStart.floorEntry(x);
        Map.Entry<Integer, Range> rightEntry = rangesByStart.ceilingEntry(x);

        Range left = leftEntry != null ? leftEntry.getValue() : null;
        Range right = rightEntry != null ? rightEntry.getValue() : null;

        // Already covered by an existing range
        if (left != null && x >= left.start && x <= left.end) {
            return;
        }

        boolean touchesLeft = left != null && left.end + 1 == x;
        boolean touchesRight = right != null && x + 1 == right.start;

        if (touchesLeft && touchesRight) {
            rangesByStart.remove(left.start);
            rangesByStart.remove(right.start);

            Range merged = new Range(left.start, right.end);
            rangesByStart.put(merged.start, merged);

        } else if (touchesLeft) {
            rangesByStart.remove(left.start);
            Range extended = new Range(left.start, x);
            rangesByStart.put(extended.start, extended);

        } else if (touchesRight) {
            rangesByStart.remove(right.start);
            Range extended = new Range(x, right.end);
            rangesByStart.put(extended.start, extended);

        } else {
            Range singleton = new Range(x, x);
            rangesByStart.put(singleton.start, singleton);
        }

        while (rangesByStart.size() > targetRangeCount) {
            mergeBestAdjacentPair();
        }
    }

    /**
     * Greedily merges the adjacent pair with the smallest gap.
     */
    private void mergeBestAdjacentPair() {
        if (rangesByStart.size() <= 1) {
            return;
        }

        Range bestLeft = null;
        Range bestRight = null;
        int bestGap = Integer.MAX_VALUE;

        Range prev = null;
        for (Range curr : rangesByStart.values()) {
            if (prev != null) {
                int gap = curr.start - prev.end - 1;
                if (gap < bestGap) {
                    bestGap = gap;
                    bestLeft = prev;
                    bestRight = curr;
                }
            }
            prev = curr;
        }

        if (bestLeft == null || bestRight == null) {
            return;
        }

        rangesByStart.remove(bestLeft.start);
        rangesByStart.remove(bestRight.start);

        Range merged = new Range(bestLeft.start, bestRight.end);
        rangesByStart.put(merged.start, merged);
    }

    public boolean contains(int x) {
        Map.Entry<Integer, Range> e = rangesByStart.floorEntry(x);
        if (e == null) {
            return false;
        }
        Range r = e.getValue();
        return x >= r.start && x <= r.end;
    }

    public int getRangeCount() {
        return rangesByStart.size();
    }

    public long getRangeBytes() {
        return (long) rangesByStart.size() * 8L;
    }

    public List<int[]> materializeRanges() {
        List<int[]> out = new ArrayList<>(rangesByStart.size());
        for (Range r : rangesByStart.values()) {
            out.add(new int[]{r.start, r.end});
        }
        return out;
    }

    private static final class Range {
        final int start;
        final int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }
}