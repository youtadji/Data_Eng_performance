package com.tu.berlin.thesis.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
            left.end = x;
        } else if (touchesRight) {
            rangesByStart.remove(right.start);

            Range newRange = new Range(x, right.end);
            rangesByStart.put(newRange.start, newRange);
        } else {
            Range singleton = new Range(x, x);
            rangesByStart.put(singleton.start, singleton);
        }

        while (rangesByStart.size() > targetRangeCount) {
            mergeBestAdjacentPair();
        }
    }

    /**
     * Regroup the currently materialized ordered ranges into clusterCount
     * adjacent groups.
     */
    public void regroupToClusterCount(int clusterCount) {
        if (clusterCount <= 0) {
            throw new IllegalArgumentException("clusterCount must be > 0");
        }

        if (rangesByStart.isEmpty()) {
            return;
        }

        if (clusterCount >= rangesByStart.size()) {
            return;
        }

        List<Range> current = new ArrayList<>(rangesByStart.values());
        TreeMap<Integer, Range> regrouped = new TreeMap<>();

        for (int g = 0; g < clusterCount; g++) {
            int from = (g * current.size()) / clusterCount;
            int toExclusive = ((g + 1) * current.size()) / clusterCount;

            Range first = current.get(from);
            Range last = current.get(toExclusive - 1);

            Range merged = new Range(first.start, last.end);
            regrouped.put(merged.start, merged);
        }

        rangesByStart.clear();
        rangesByStart.putAll(regrouped);

        while (rangesByStart.size() > targetRangeCount) {
            mergeBestAdjacentPair();
        }
    }

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
        List<int[]> out = new ArrayList<>();
        for (Range r : rangesByStart.values()) {
            out.add(new int[]{r.start, r.end});
        }
        return out;
    }

    private static final class Range {
        int start;
        int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }
}