package com.tu.berlin.thesis.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Streaming approximate range set for hash join prefiltering.
 *
 * Two-layer design:
 *   rawRanges     — exact adjacency-merged ranges, never approximated.
 *                   Ground truth of everything inserted so far.
 *   rangesByStart — the current approximate view.
 *
 * During streaming insert:
 *   - rawRanges is always exact
 *   - rangesByStart is kept exact while rawRanges.size() <= targetRangeCount
 *   - once rawRanges grows beyond targetRangeCount, rangesByStart is rebuilt
 *     with a globally optimal DP partition into targetRangeCount groups
 *
 * Finalization:
 *   finalizeToTargetRangeCount() recomputes the final approximate view
 *   directly from rawRanges, so it never re-approximates an approximation.
 *
 * Objective:
 *   Minimize total swallowed gap waste, i.e. false-positive surface.
 */
public class IntStreamingApproximateRangeSet {

    private final int targetRangeCount;

    private final TreeMap<Integer, Range> rawRanges     = new TreeMap<>();
    private final TreeMap<Integer, Range> rangesByStart = new TreeMap<>();

    public IntStreamingApproximateRangeSet(int targetRangeCount) {
        if (targetRangeCount <= 0) {
            throw new IllegalArgumentException("targetRangeCount must be > 0");
        }
        this.targetRangeCount = targetRangeCount;
    }

    // -------------------------------------------------------------------------
    // Streaming insert — called once per build-side key
    // -------------------------------------------------------------------------

    public void insert(int x) {
        boolean changed = insertIntoRaw(x);
        if (!changed) {
            return;
        }

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

    // Optional helper for debugging / other experiments
    public void regroupToClusterCount(int clusterCount) {
        if (clusterCount <= 0) {
            throw new IllegalArgumentException("clusterCount must be > 0");
        }
        rangesByStart.clear();
        rangesByStart.putAll(computeOptimalPartition(clusterCount));
    }

    // -------------------------------------------------------------------------
    // Probe — called per probe-side tuple
    // -------------------------------------------------------------------------

    public boolean contains(int x) {
        Map.Entry<Integer, Range> e = rangesByStart.floorEntry(x);
        if (e == null) {
            return false;
        }
        Range r = e.getValue();
        return x >= r.start && x <= r.end;
    }

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    public int getRangeCount() {
        return rangesByStart.size();
    }

    public int getRawRangeCount() {
        return rawRanges.size();
    }

    public long getRangeBytes() {
        return (long) rangesByStart.size() * 8L;
    }

    /**
     * Total gap waste of the approximate view.
     * = number of integers falsely covered by approximate ranges.
     */
    public long getTotalGapWaste() {
        long waste = 0;
        for (Range r : rangesByStart.values()) {
            waste += gapWasteOf(r);
        }
        return waste;
    }

    /**
     * False positive rate estimate over a domain [domainMin, domainMax].
     */
    public double estimateFalsePositiveRate(int domainMin, int domainMax) {
        long domainSize = (long) domainMax - domainMin + 1L;
        if (domainSize <= 0) {
            return 0.0;
        }
        return (double) getTotalGapWaste() / domainSize;
    }

    public List<int[]> materializeRanges() {
        List<int[]> out = new ArrayList<>(rangesByStart.size());
        for (Range r : rangesByStart.values()) {
            out.add(new int[]{r.start, r.end});
        }
        return out;
    }

    public List<int[]> materializeRawRanges() {
        List<int[]> out = new ArrayList<>(rawRanges.size());
        for (Range r : rawRanges.values()) {
            out.add(new int[]{r.start, r.end});
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Raw layer insert
    // Returns true iff rawRanges actually changed.
    // -------------------------------------------------------------------------

    private boolean insertIntoRaw(int x) {
        Map.Entry<Integer, Range> leftEntry  = rawRanges.floorEntry(x);
        Map.Entry<Integer, Range> rightEntry = rawRanges.ceilingEntry(x);

        Range left  = leftEntry  != null ? leftEntry.getValue()  : null;
        Range right = rightEntry != null ? rightEntry.getValue() : null;

        // Already covered
        if (left != null && x >= left.start && x <= left.end) {
            return false;
        }

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
        for (Range r : rawRanges.values()) {
            rangesByStart.put(r.start, r);
        }
    }

    // -------------------------------------------------------------------------
    // Interval DP — globally optimal K-partition of rawRanges
    //
    // dp[i][k] = min gap waste to cover atoms[0..i-1] in exactly k groups
    //
    // Recurrence:
    //   dp[i][k] = min over j in [k-1, i-1] of:
    //              dp[j][k-1] + waste(j, i-1)
    //
    // waste(from, to) = span of merged range - total integers covered within it
    // -------------------------------------------------------------------------

    private TreeMap<Integer, Range> computeOptimalPartition(int K) {
        List<Range> atoms = new ArrayList<>(rawRanges.values());
        int n = atoms.size();

        TreeMap<Integer, Range> result = new TreeMap<>();
        if (n == 0) {
            return result;
        }

        if (n <= K) {
            for (Range r : atoms) {
                result.put(r.start, r);
            }
            return result;
        }

        long[] prefixCovered = new long[n + 1];
        for (int i = 0; i < n; i++) {
            prefixCovered[i + 1] = prefixCovered[i]
                    + (atoms.get(i).end - atoms.get(i).start + 1L);
        }

        final long INF = Long.MAX_VALUE / 4L;

        long[][] dp = new long[n + 1][K + 1];
        int[][] split = new int[n + 1][K + 1];

        for (long[] row : dp) {
            Arrays.fill(row, INF);
        }
        dp[0][0] = 0L;

        for (int i = 1; i <= n; i++) {
            dp[i][1] = waste(atoms, prefixCovered, 0, i - 1);
            split[i][1] = 0;

            for (int k = 2; k <= Math.min(i, K); k++) {
                for (int j = k - 1; j < i; j++) {
                    if (dp[j][k - 1] == INF) {
                        continue;
                    }

                    long candidate = dp[j][k - 1]
                            + waste(atoms, prefixCovered, j, i - 1);

                    if (candidate < dp[i][k]) {
                        dp[i][k] = candidate;
                        split[i][k] = j;
                    }
                }
            }
        }

        int i = n;
        int k = K;
        while (k > 0 && i > 0) {
            int j = split[i][k];
            Range merged = new Range(atoms.get(j).start, atoms.get(i - 1).end);
            result.put(merged.start, merged);
            i = j;
            k--;
        }

        return result;
    }

    private long waste(List<Range> atoms, long[] prefixCovered, int from, int to) {
        long span = atoms.get(to).end - atoms.get(from).start + 1L;
        long covered = prefixCovered[to + 1] - prefixCovered[from];
        return span - covered;
    }

    private long gapWasteOf(Range r) {
        long span = r.end - r.start + 1L;
        long covered = 0L;

        for (Range raw : rawRanges.subMap(r.start, true, r.end, true).values()) {
            covered += raw.end - raw.start + 1L;
        }

        return span - covered;
    }

    // -------------------------------------------------------------------------
    // Immutable range
    // -------------------------------------------------------------------------

    private static final class Range {
        final int start;
        final int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
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