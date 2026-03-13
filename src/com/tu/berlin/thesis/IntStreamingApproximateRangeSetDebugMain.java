package com.tu.berlin.thesis;

import com.tu.berlin.thesis.operators.IntStreamingApproximateRangeSet;

import java.util.List;

public class IntStreamingApproximateRangeSetDebugMain {

    public static void main(String[] args) {
        System.out.println("=== DEBUG: IntStreamingApproximateRangeSet ===");

        runScenario(
                "Scenario 1: me example",
                2,
                new int[]{10, 11, 20, 21, 30}
        );

        runScenario(
                "Scenario 2: bridge two ranges",
                10,
                new int[]{10, 11, 20, 21, 12}
        );

        runScenario(
                "Scenario 3: isolated inserts then forced merge",
                2,
                new int[]{10, 20, 30, 40}
        );

        runScenario(
                "Scenario 4: duplicate + inside existing range",
                10,
                new int[]{10, 11, 12, 11, 12, 10}
        );

        runScenario(
                "Scenario 5: touches right side",
                10,
                new int[]{30, 31, 29}
        );

        runScenario(
                "Scenario 6: longer evolving example",
                3,
                new int[]{10, 11, 20, 21, 15, 16, 17, 18, 19, 12, 13, 14}
        );
    }

    private static void runScenario(String title, int targetRangeCount, int[] keys) {
        System.out.println();
        System.out.println("--------------------------------------------------");
        System.out.println(title);
        System.out.println("targetRangeCount = " + targetRangeCount);

        IntStreamingApproximateRangeSet set =
                new IntStreamingApproximateRangeSet(targetRangeCount);

        for (int key : keys) {
            System.out.println();
            System.out.println("Insert: " + key);
            set.insert(key);

            printRanges(set);
            System.out.println("rangeCount = " + set.getRangeCount());
            System.out.println("rangeBytes = " + set.getRangeBytes());
        }
    }

    private static void printRanges(IntStreamingApproximateRangeSet set) {
        List<int[]> ranges = set.materializeRanges();

        System.out.print("Ranges: ");
        if (ranges.isEmpty()) {
            System.out.println("(empty)");
            return;
        }

        for (int i = 0; i < ranges.size(); i++) {
            int[] r = ranges.get(i);
            System.out.print("[" + r[0] + ", " + r[1] + "]");
            if (i < ranges.size() - 1) {
                System.out.print(" ");
            }
        }
        System.out.println();
    }
}