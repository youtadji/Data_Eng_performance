package com.tu.berlin.thesis.rangetree;

import java.util.Arrays;

/**
 * Collects build keys (int), sorts them, and compresses them into
 * exact consecutive ranges [start,end].
 *
 * Output ranges are sorted by start.
 *
 * Exact = no false positives, no false negatives for membership in the union of ranges.
 */
public final class RangeExtractor {

    /** Result container: ranges[i] = [starts[i], ends[i]] for i in [0..count-1] */
    public static final class Ranges {
        public final int[] starts;
        public final int[] ends;
        public final int count;

        public Ranges(int[] starts, int[] ends, int count) {
            this.starts = starts;
            this.ends = ends;
            this.count = count;
        }
    }

    private int[] keys;
    private int n;

    public RangeExtractor(int expectedKeys) {
        this.keys = new int[Math.max(16, expectedKeys)];
        this.n = 0;
    }

    /** Add one build-side key (called during build phase). */
    public void add(int key) {
        if (n == keys.length) {
            keys = Arrays.copyOf(keys, keys.length * 2);
        }
        keys[n++] = key;
    }

    /**
     * Build exact ranges from the collected keys.
     * After calling this, you typically don't add more keys.
     */
    public Ranges buildExactRanges() {
        if (n == 0) {
            return new Ranges(new int[0], new int[0], 0);
        }

        Arrays.sort(keys, 0, n);

        int[] starts = new int[n];
        int[] ends = new int[n];
        int r = 0;

        int start = keys[0];
        int prev = keys[0];

        for (int i = 1; i < n; i++) {
            int x = keys[i];

            if (x == prev) {
                continue; // duplicate
            }

            if (x == prev + 1) {
                prev = x;
            } else {
                starts[r] = start;
                ends[r] = prev;
                r++;

                start = x;
                prev = x;
            }
        }

        starts[r] = start;
        ends[r] = prev;
        r++;

        if (r < n) {
            starts = Arrays.copyOf(starts, r);
            ends = Arrays.copyOf(ends, r);
        }

        return new Ranges(starts, ends, r);
    }

    /**
     * Regroup adjacent exact ranges into a requested number of chunk groups.
     *
     * Example:
     * - exact ranges count = 256
     * - targetClusters = 64
     * => merge adjacent ranges into 64 groups
     *
     * This does NOT change the data.
     * It only changes how the already-extracted exact ranges are grouped.
     */
    public static Ranges regroupToTargetClusters(Ranges exact, int targetClusters) {
        if (exact.count == 0) {
            return exact;
        }

        if (targetClusters <= 0) {
            throw new IllegalArgumentException("targetClusters must be > 0");
        }

        if (targetClusters >= exact.count) {
            return exact;
        }

        int[] starts = new int[targetClusters];
        int[] ends = new int[targetClusters];

        for (int g = 0; g < targetClusters; g++) {
            int from = (g * exact.count) / targetClusters;
            int toExclusive = ((g + 1) * exact.count) / targetClusters;

            starts[g] = exact.starts[from];
            ends[g] = exact.ends[toExclusive - 1];
        }

        return new Ranges(starts, ends, targetClusters);
    }
}