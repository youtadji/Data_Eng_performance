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

        // first we ll sort all keys
        Arrays.sort(keys, 0, n);

        // worst case: each key isolated => number of ranges = n
        int[] starts = new int[n];
        int[] ends = new int[n];
        int r = 0;

        // then we gonna scan sorted keys and create ranges
        int start = keys[0];
        int prev = keys[0];

        for (int i = 1; i < n; i++) {
            int x = keys[i];

            if (x == prev) {
                // duplicate key: ignore it
                continue;
            }

            if (x == prev + 1) {
                // consecutive: extend current range
                prev = x;
            } else {
                // gap: close current range and start a new one
                starts[r] = start;
                ends[r] = prev;
                r++;

                start = x;
                prev = x;
            }
        }

        // close final range
        starts[r] = start;
        ends[r] = prev;
        r++;

        // shrink arrays to actual count ?? bof ?
        if (r < n) {
            starts = Arrays.copyOf(starts, r);
            ends = Arrays.copyOf(ends, r);
        }

        return new Ranges(starts, ends, r);
    }
}
