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

        // Case 1: already inside an existing range
        if (left != null && x >= left.start && x <= left.end) {
            return;
        }

        boolean touchesLeft = left != null && left.end + 1 == x;
        boolean touchesRight = right != null && x + 1 == right.start;

        // Case 4: bridges left and right   ex [10,12]   [14,20]
        //insert: 13   -> merge to [10,20]
        if (touchesLeft && touchesRight) {
            rangesByStart.remove(left.start);
            rangesByStart.remove(right.start);

            Range merged = new Range(left.start, right.end);
            rangesByStart.put(merged.start, merged);
        }
        // Case 2: touches left
        else if (touchesLeft) {
            left.end = x;
        }
        // Case 3: touches right
        else if (touchesRight) {
            rangesByStart.remove(right.start);

            Range newRange = new Range(x, right.end);
            rangesByStart.put(newRange.start, newRange);
        }
        // Case 5: isolated new key
        else {
            Range singleton = new Range(x, x);
            rangesByStart.put(singleton.start, singleton);
        }

        // we appproximate immediately if range budget is exceeded
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
        // 2 ints per range = start + end = 8 bytes logical payload
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