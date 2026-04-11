package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.util.List;

public class IntSmallMainSanity {

    public static void main(String[] args) throws Exception {
        System.out.println("=== SMALL SANITY RUN ===");

        int buildSize = 1000;
        int probeSize = 10000;
        double selectivity = 0.05;

        int[] clusterCounts = {16, 32};
        int[] targetRangeCounts = {4, 8, 12};

        String prefix = "sanity_B" + buildSize
                + "_P" + probeSize
                + "_S" + (int) (selectivity * 100);

        generateFixedDataIfMissing(buildSize, probeSize, selectivity, prefix);

        List<int[]> dates = IntCSVReader.readCSV("data/" + prefix + "_dates_int.csv");
        List<int[]> sales = IntCSVReader.readCSV("data/" + prefix + "_sales_int.csv");

        for (int clusters : clusterCounts) {
            for (int targetRangeCount : targetRangeCounts) {

                if (targetRangeCount >= clusters) {
                    continue;
                }

                System.out.println("\n--- RUN ---");

                ApproxRunTimed bulk = runApproxRangesBuildAndProbe(
                        dates,
                        sales,
                        dates.size(),
                        clusters,
                        targetRangeCount
                );

                ApproxStreamedRunTimed streamed = runApproxRangesStreamedBuildAndProbe(
                        dates,
                        sales,
                        dates.size(),
                        clusters,
                        targetRangeCount
                );

                System.out.println(
                        "clusters=" + clusters +
                                ", target=" + targetRangeCount +
                                ", bulkRanges=" + bulk.approxRangeCount +
                                ", streamedRanges=" + streamed.approxRangeCount +
                                ", bulkLookups=" + bulk.hashLookups +
                                ", streamedLookups=" + streamed.hashLookups
                );
            }
        }

        System.out.println("\nDONE SANITY RUN");
    }

    private static ApproxRunTimed runApproxRangesBuildAndProbe(
            List<int[]> dates,
            List<int[]> sales,
            int expectedBuildKeys,
            int clusterCount,
            int targetRangeCount
    ) {
        IntHashJoinWithApproximateRanges join = new IntHashJoinWithApproximateRanges(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys,
                clusterCount,
                targetRangeCount
        );

        join.open();
        while (join.next() != null) { }
        join.close();

        return new ApproxRunTimed(
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getExactRangeCount(),
                join.getGroupedRangeCount(),
                join.getApproximateRangeCount()
        );
    }

    private static ApproxStreamedRunTimed runApproxRangesStreamedBuildAndProbe(
            List<int[]> dates,
            List<int[]> sales,
            int expectedBuildKeys,
            int clusterCount,
            int targetRangeCount
    ) {
        IntHashJoinWithApproximateRangesStreamed join = new IntHashJoinWithApproximateRangesStreamed(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys,
                clusterCount,
                targetRangeCount
        );

        join.open();
        while (join.next() != null) { }
        join.close();

        return new ApproxStreamedRunTimed(
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getApproximateRangeCount()
        );
    }

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateFixedDataIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            String prefix
    ) {
        String datesPath = "data/" + prefix + "_dates_int.csv";
        String salesPath = "data/" + prefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("Fixed data exists -> " + prefix);
            return;
        }

        System.out.println("Generating fixed BLOCK+GAP data -> " + prefix);

        try {
            int base = 1_000_000_000;

            int blockLen = 100;
            int gapLen = 20;
            int stride = blockLen + gapLen;

            int[] validIds = new int[buildSize];
            int validCount = 0;

            java.util.ArrayList<Integer> nearMissIds = new java.util.ArrayList<>();

            try (java.io.PrintWriter w = new java.io.PrintWriter(datesPath)) {
                w.println("id,year,desc_code");

                int blockIndex = 0;

                while (validCount < buildSize) {
                    int blockStart = base + blockIndex * stride;

                    for (int off = 0; off < blockLen && validCount < buildSize; off++) {
                        int id = blockStart + off;
                        validIds[validCount++] = id;

                        int year = 1980 + (id % 40);
                        int descCode = id % 1000;

                        w.println(id + "," + year + "," + descCode);
                    }

                    int gapStart = blockStart + blockLen;
                    for (int g = 0; g < gapLen; g++) {
                        nearMissIds.add(gapStart + g);
                    }

                    blockIndex++;
                }
            }

            try (java.io.PrintWriter w = new java.io.PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                for (int i = 1; i <= matchCount; i++) {
                    int id = validIds[(i - 1) % validCount];
                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                int remaining = probeSize - matchCount;
                int nearMissCount = remaining / 2;
                int farMissCount = remaining - nearMissCount;

                int rowId = matchCount + 1;

                for (int t = 0; t < nearMissCount; t++) {
                    int nearMissId = nearMissIds.get(t % nearMissIds.size());
                    w.println(rowId + "," + nearMissId + "," + (rowId % 5000) + ",10," + (rowId % 20000));
                    rowId++;
                }

                int nonMatchBase = 2_000_000_000;
                for (int t = 0; t < farMissCount; t++) {
                    int nonMatchId = nonMatchBase + t + 1;
                    w.println(rowId + "," + nonMatchId + "," + (rowId % 5000) + ",10," + (rowId % 20000));
                    rowId++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final class ApproxRunTimed {
        final int hashLookups;
        final int passes;
        final int rejects;
        final int exactRangeCount;
        final int groupedRangeCount;
        final int approxRangeCount;

        ApproxRunTimed(int hashLookups, int passes, int rejects,
                       int exactRangeCount, int groupedRangeCount, int approxRangeCount) {
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.exactRangeCount = exactRangeCount;
            this.groupedRangeCount = groupedRangeCount;
            this.approxRangeCount = approxRangeCount;
        }
    }

    private static final class ApproxStreamedRunTimed {
        final int hashLookups;
        final int passes;
        final int rejects;
        final int approxRangeCount;

        ApproxStreamedRunTimed(int hashLookups, int passes, int rejects, int approxRangeCount) {
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.approxRangeCount = approxRangeCount;
        }
    }
}