package com.tu.berlin.thesis;

import com.tu.berlin.thesis.operators.IntHashJoinWithApproximateRanges;
import com.tu.berlin.thesis.operators.IntScanOperator;

import java.util.ArrayList;
import java.util.List;

public class IntApproxJoinTinyTest {

    public static void main(String[] args) {

        // -----------------------------
        // Build side: key at column 0
        // -----------------------------
        List<int[]> build = new ArrayList<>();
        build.add(new int[]{1});
        build.add(new int[]{2});
        build.add(new int[]{3});
        build.add(new int[]{4});
        build.add(new int[]{5});
        build.add(new int[]{8});
        build.add(new int[]{9});
        build.add(new int[]{10});
        build.add(new int[]{20});
        build.add(new int[]{21});
        build.add(new int[]{22});

        // -----------------------------
        // Probe side: join key at column 1
        // -----------------------------
        List<int[]> probe = new ArrayList<>();
        probe.add(new int[]{100, 1});
        probe.add(new int[]{101, 6});
        probe.add(new int[]{102, 7});
        probe.add(new int[]{103, 8});
        probe.add(new int[]{104, 15});
        probe.add(new int[]{105, 20});
        probe.add(new int[]{106, 30});

        IntHashJoinWithApproximateRanges join =
                new IntHashJoinWithApproximateRanges(
                        new IntScanOperator(build),
                        new IntScanOperator(probe),
                        0,   // build key index
                        1,   // probe key index
                        build.size(),
                        2    // target approximate range count
                );

        join.open();

        System.out.println("=== JOIN OUTPUT ===");
        int[] out;
        int outputCount = 0;

        while ((out = join.next()) != null) {
            outputCount++;
            System.out.print("row: ");
            for (int i = 0; i < out.length; i++) {
                System.out.print(out[i]);
                if (i < out.length - 1) System.out.print(", ");
            }
            System.out.println();
        }

        join.close();

        System.out.println("=== METRICS ===");
        System.out.println("outputCount           = " + outputCount);
        System.out.println("exactRangeCount       = " + join.getExactRangeCount());
        System.out.println("approxRangeCount      = " + join.getApproximateRangeCount());
        System.out.println("rangePasses           = " + join.getRangePasses());
        System.out.println("rangeRejects          = " + join.getRangeRejects());
        System.out.println("hashLookups           = " + join.getHashLookups());
        System.out.println("actualMatches         = " + join.getActualMatches());
        System.out.println("rangeBytes            = " + join.getRangeBytes());
    }
}