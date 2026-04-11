package com.tu.berlin.thesis.operators;

import java.util.*;

public class IntStreamingApproximateRangeSet {

    private final int targetRangeCount;

    private final TreeMap<Integer, Range> rawRanges     = new TreeMap<>();
    private final TreeMap<Integer, Range> rangesByStart = new TreeMap<>();

    public IntStreamingApproximateRangeSet(int targetRangeCount) {
        if (targetRangeCount <= 0)
            throw new IllegalArgumentException("targetRangeCount must be > 0");
        this.targetRangeCount = targetRangeCount;
    }

    // -------------------------------------------------------------------------
    // Streaming insert — called once per build-side key
    // -------------------------------------------------------------------------

    public void insert(int x) {
        boolean changed = insertIntoRaw(x);
        if (!changed) return;

        if (rawRanges.size() > targetRangeCount) {
            reoptimize();
        } else {
            rebuildFromRaw();
        }
    }

    // -------------------------------------------------------------------------
    // Finalize approximate view to the experiment target.
    // Always works from rawRanges so it never re-approximates an approximation.
    // -------------------------------------------------------------------------

    public void finalizeToTargetRangeCount() {
        rangesByStart.clear();
        rangesByStart.putAll(computeOptimalPartition(targetRangeCount));
    }

    // Optional: keep only if you still want this for debugging/other experiments
    public void regroupToClusterCount(int clusterCount) {
        if (clusterCount <= 0)
            throw new IllegalArgumentException("clusterCount must be > 0");
        rangesByStart.clear();
        rangesByStart.putAll(computeOptimalPartition(clusterCount));
    }

    // -------------------------------------------------------------------------
    // Probe — called per probe-side tuple
    // -------------------------------------------------------------------------

    public boolean contains(int x) {
        Map.Entry<Integer, Range> e = rangesByStart.floorEntry(x);
        if (e == null) return false;
        Range r = e.getValue();
        return x >= r.start && x <= r.end;
    }

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    public int getRangeCount()    { return rangesByStart.size(); }
    public int getRawRangeCount() { return rawRanges.size(); }
    public long getRangeBytes()   { return (long) rangesByStart.size() * 8L; }

    public long getTotalGapWaste() {
        long waste = 0;
        for (Range r : rangesByStart.values())
            waste += gapWasteOf(r);
        return waste;
    }

    public double estimateFalsePositiveRate(int domainMin, int domainMax) {
        long domainSize = (long) domainMax - domainMin + 1;
        if (domainSize <= 0) return 0.0;
        return (double) getTotalGapWaste() / domainSize;
    }

    public List<int[]> materializeRanges() {
        List<int[]> out = new ArrayList<>(rangesByStart.size());
        for (Range r : rangesByStart.values())
            out.add(new int[]{ r.start, r.end });
        return out;
    }

    public List<int[]> materializeRawRanges() {
        List<int[]> out = new ArrayList<>(rawRanges.size());
        for (Range r : rawRanges.values())
            out.add(new int[]{ r.start, r.end });
        return out;
    }

    // -------------------------------------------------------------------------
    // Raw layer insert
    // -------------------------------------------------------------------------

    private boolean insertIntoRaw(int x) {
        Map.Entry<Integer, Range> leftEntry  = rawRanges.floorEntry(x);
        Map.Entry<Integer, Range> rightEntry = rawRanges.ceilingEntry(x);

        Range left  = leftEntry  != null ? leftEntry.getValue()  : null;
        Range right = rightEntry != null ? rightEntry.getValue() : null;

        if (left != null && x >= left.start && x <= left.end) return false;

        boolean touchesLeft  = left  != null && left.end + 1 == x;
        boolean touchesRight = right != null && x + 1 == right.start;

        if (touchesLeft && touchesRight) {
            rawRanges.remove(left.start);
            rawRanges.remove(right.start);
            rawRanges.put(left.start, new Range(left.start, right.end));
        } else if (touchesLeft) {
            rawRanges.remove(left.start);
            rawRanges.put(left.start, new Range(left.start, x));
        } else if (touchesRight) {
            rawRanges.remove(right.start);
            rawRanges.put(x, new Range(x, right.end));
        } else {
            rawRanges.put(x, new Range(x, x));
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Approximate view management
    // -------------------------------------------------------------------------

    private void reoptimize() {
        rangesByStart.clear();
        rangesByStart.putAll(computeOptimalPartition(targetRangeCount));
    }

    private void rebuildFromRaw() {
        rangesByStart.clear();
        for (Range r : rawRanges.values())
            rangesByStart.put(r.start, r);
    }

    // -------------------------------------------------------------------------
    // Interval DP
    // -------------------------------------------------------------------------

    private TreeMap<Integer, Range> computeOptimalPartition(int K) {
        List<Range> atoms = new ArrayList<>(rawRanges.values());
        int N = atoms.size();

        if (N == 0) return new TreeMap<>();

        if (N <= K) {
            TreeMap<Integer, Range> result = new TreeMap<>();
            for (Range r : atoms) result.put(r.start, r);
            return result;
        }

        long[] prefixSize = new long[N + 1];
        for (int i = 0; i < N; i++)
            prefixSize[i + 1] = prefixSize[i]
                    + (atoms.get(i).end - atoms.get(i).start + 1L);

        final long INF = Long.MAX_VALUE / 2;

        long[][] dp    = new long[N + 1][K + 1];
        int[][]  split = new int[N + 1][K + 1];

        for (long[] row : dp) Arrays.fill(row, INF);
        dp[0][0] = 0;

        for (int i = 1; i <= N; i++) {
            dp[i][1] = waste(atoms, prefixSize, 0, i - 1);
            split[i][1] = 0;

            for (int k = 2; k <= Math.min(i, K); k++) {
                for (int j = k - 1; j < i; j++) {
                    if (dp[j][k - 1] == INF) continue;
                    long candidate = dp[j][k - 1]
                            + waste(atoms, prefixSize, j, i - 1);
                    if (candidate < dp[i][k]) {
                        dp[i][k] = candidate;
                        split[i][k] = j;
                    }
                }
            }
        }

        TreeMap<Integer, Range> result = new TreeMap<>();
        int i = N, k = K;
        while (k > 0 && i > 0) {
            int j = split[i][k];
            result.put(
                    atoms.get(j).start,
                    new Range(atoms.get(j).start, atoms.get(i - 1).end)
            );
            i = j;
            k--;
        }
        return result;
    }

    private long waste(List<Range> atoms, long[] prefixSize, int from, int to) {
        long span    = atoms.get(to).end - atoms.get(from).start + 1L;
        long covered = prefixSize[to + 1] - prefixSize[from];
        return span - covered;
    }

    private long gapWasteOf(Range r) {
        long span    = r.end - r.start + 1L;
        long covered = 0;
        for (Range raw : rawRanges.subMap(r.start, true, r.end, true).values())
            covered += raw.end - raw.start + 1L;
        return span - covered;
    }

    private static final class Range {
        final int start;
        final int end;

        Range(int start, int end) {
            this.start = start;
            this.end   = end;
        }
    }
}

/*
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
*/