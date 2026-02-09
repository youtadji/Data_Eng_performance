package com.tu.berlin.thesis.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class TupleSanityChecker {

    public static void check(
            List<int[]> build,
            List<int[]> probe,
            int expectedBuildSize,
            int expectedProbeSize,
            double selectivity,
            String tag
    ) {
        // -------------------------
        // 1) Row counts
        // -------------------------
        if (build.size() != expectedBuildSize) {
            throw new IllegalStateException(
                    tag + " ❌ build rows = " + build.size() +
                            " expected " + expectedBuildSize
            );
        }

        if (probe.size() != expectedProbeSize) {
            throw new IllegalStateException(
                    tag + " ❌ probe rows = " + probe.size() +
                            " expected " + expectedProbeSize
            );
        }

        // -------------------------
        // 2) Collect build keys
        // -------------------------
        Set<Integer> buildKeys = new HashSet<>(build.size() * 2);
        for (int[] r : build) {
            buildKeys.add(r[0]); // build key column
        }

        // -------------------------
        // 3) Count matches in probe
        // -------------------------
        long matches = 0;
        for (int[] r : probe) {
            int key = r[1]; // probe key column
            if (buildKeys.contains(key)) matches++;
        }

        long expectedMatches = Math.round(expectedProbeSize * selectivity);

        if (Math.abs(matches - expectedMatches) > 10) {
            throw new IllegalStateException(
                    tag + " ❌ probe matches = " + matches +
                            " expected ~" + expectedMatches
            );
        }

        // -------------------------
        // 4) Non-match overlap check
        // -------------------------
        long falseMatches = 0;
        for (int i = expectedMatches; i < probe.size(); i++) {
            int key = probe.get(i)[1];
            if (buildKeys.contains(key)) falseMatches++;
        }

        if (falseMatches != 0) {
            throw new IllegalStateException(
                    tag + " ❌ non-match overlap = " + falseMatches
            );
        }

        // -------------------------
        // 5) OK
        // -------------------------

    }
}
