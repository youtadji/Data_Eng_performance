package com.tu.berlin.thesis.rangetree;

import java.util.Arrays;

/**
 * Approximates exact sorted non-overlapping ranges by greedily merging
 * adjacent ranges with the smallest gap until targetRangeCount is reached.
 *
 * No false negatives.
 * False positives are allowed.
 */
public final class RangeApproximator {

    private RangeApproximator() {}

    public static RangeExtractor.Ranges approximate(
            RangeExtractor.Ranges exact,
            int targetRangeCount
    ) {
        if (exact == null) {
            throw new IllegalArgumentException("exact must not be null");
        }

        int n = exact.count;

        if (n == 0) {
            return new RangeExtractor.Ranges(new int[0], new int[0], 0);
        }

        if (targetRangeCount < 1) {
            throw new IllegalArgumentException("targetRangeCount must be >= 1");
        }

        if (targetRangeCount >= n) {
            return new RangeExtractor.Ranges(
                    Arrays.copyOf(exact.starts, n),
                    Arrays.copyOf(exact.ends, n),
                    n
            );
        }

        int[] starts = Arrays.copyOf(exact.starts, n);
        int[] ends = Arrays.copyOf(exact.ends, n);
        int count = n;

        while (count > targetRangeCount) {
            int bestIdx = -1;
            int bestGap = Integer.MAX_VALUE;

            for (int i = 0; i < count - 1; i++) {
                int gap = starts[i + 1] - ends[i] - 1;
                if (gap < bestGap) {
                    bestGap = gap;
                    bestIdx = i;
                }
            }

            // merge bestIdx and bestIdx + 1
            ends[bestIdx] = ends[bestIdx + 1];

            for (int j = bestIdx + 1; j < count - 1; j++) {
                starts[j] = starts[j + 1];
                ends[j] = ends[j + 1];
            }

            count--;
        }

        return new RangeExtractor.Ranges(
                Arrays.copyOf(starts, count),
                Arrays.copyOf(ends, count),
                count
        );
    }

    // ============================================================
    // TEMP TEST MAIN
    // Run this class directly
    // ============================================================
    public static void main(String[] args) {
        RangeExtractor extractor = new RangeExtractor(16);

        int[] keys = {1, 2, 3, 4, 5, 8, 9, 10, 20, 21, 22};
        for (int k : keys) {
            extractor.add(k);
        }

        RangeExtractor.Ranges exact = extractor.buildExactRanges();

        System.out.println("=== EXACT RANGES ===");
        printRanges(exact);

        RangeExtractor.Ranges approx = approximate(exact, 2);

        System.out.println("=== APPROX RANGES (target=2) ===");
        printRanges(approx);

        ExactRangesIndex exactIndex = new ExactRangesIndex();
        exactIndex.build(exact.starts, exact.ends, exact.count);

        ExactRangesIndex approxIndex = new ExactRangesIndex();
        approxIndex.build(approx.starts, approx.ends, approx.count);

        int[] testKeys = {1, 5, 6, 7, 8, 10, 15, 20, 22, 30};

        System.out.println("=== MEMBERSHIP TEST ===");
        for (int key : testKeys) {
            boolean inExact = exactIndex.contains(key);
            boolean inApprox = approxIndex.contains(key);

            System.out.println(
                    "key=" + key +
                            " | exact=" + inExact +
                            " | approx=" + inApprox
            );
        }
    }

    private static void printRanges(RangeExtractor.Ranges r) {
        for (int i = 0; i < r.count; i++) {
            System.out.println("[" + r.starts[i] + ", " + r.ends[i] + "]");
        }
    }
}