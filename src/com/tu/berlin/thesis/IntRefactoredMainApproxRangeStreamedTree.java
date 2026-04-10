package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IntRefactoredMainApproxRangeStreamedTree {

    public static void main(String[] args) {
        System.out.println("=== EXP INT: NoFilter vs Bloom vs ExactRanges vs BulkApprox vs StreamedApprox ===");
        runExperiment_Int("controlled");
    }

    private static void runExperiment_Int(String suffix) {

        int[] buildSizes = {1_000_000};

        int[] bloomSizes = {
                160_000_000,
                1_600_000_000
        };

        int[] clusterCounts = {16, 32, 64, 128, 256};
        int[] targetRangeCounts = {4, 8, 10, 12, 16, 20, 25, 32, 40, 50, 54, 64, 80, 100, 110, 160, 200, 240};
        int[] gapLens = {100, 1000, 10_000, 100_000};

        int probeSize = 10_000_000;
        double selectivity = 0.05;
        int k = 2;

        String outName = "experiment_streamed_vs_bulk_approxranges_clean_" + suffix + ".csv";

        try (PrintWriter w = new PrintWriter(outName)) {

            w.println(
                    "build_size,probe_size,filter_size_bits,clusters,cluster_len,gap_len,target_ranges,"
                            + "build_ms_no_filter,probe_ms_no_filter,"
                            + "build_ms_bloom,probe_ms_bloom,"
                            + "build_ms_exactranges,probe_ms_exactranges,"
                            + "build_ms_approxranges_bulk,probe_ms_approxranges_bulk,"
                            + "build_ms_approxranges_streamed,probe_ms_approxranges_streamed,"
                            + "speedup_probe_bloom,speedup_probe_exactranges,speedup_probe_approx_bulk,speedup_probe_approx_streamed,"
                            + "hash_lookups_bloom,bloom_passes,bloom_rejects,"
                            + "hash_lookups_range,range_passes,range_rejects,range_count,range_bytes,"
                            + "hash_lookups_approx_bulk,approx_bulk_passes,approx_bulk_rejects,exact_range_count,approx_range_count_bulk,approx_range_bytes_bulk,"
                            + "hash_lookups_approx_streamed,approx_streamed_passes,approx_streamed_rejects,approx_range_count_streamed,approx_range_bytes_streamed,"
                            + "approx_lookup_ratio_bulk,approx_lookup_ratio_streamed"
            );

            for (int buildSize : buildSizes) {
                for (int gapLen : gapLens) {
                    for (int clusters : clusterCounts) {

                        int clusterLen = buildSize / clusters;

                        String prefix = "exp_streamed_compare"
                                + "_G" + gapLen
                                + "_C" + clusters
                                + "_L" + clusterLen
                                + "_B" + buildSize;

                        // ============================================================
                        // Generate once per dataset configuration
                        // ============================================================
                        generateIntDataWithClustersAndGapNonMatchesIfMissing(
                                buildSize,
                                probeSize,
                                selectivity,
                                prefix,
                                clusters,
                                clusterLen,
                                gapLen
                        );

                        // ============================================================
                        // Read once per dataset configuration
                        // ============================================================
                        List<int[]> dates = IntCSVReader.readCSV("data/" + prefix + "_dates_int.csv");
                        List<int[]> sales = IntCSVReader.readCSV("data/" + prefix + "_sales_int.csv");

                        // ============================================================
                        // Run dataset-independent baselines once
                        // ============================================================
                        TimedRun noF = runNoFilterBuildAndProbe(dates, sales);
                        RangeRunTimed rt = runExactRangesBuildAndProbe(dates, sales, dates.size());

                        // ============================================================
                        // Run Bloom once per dataset per Bloom size
                        // ============================================================
                        Map<Integer, BloomRunTimed> bloomResults = new HashMap<>();
                        for (int m : bloomSizes) {
                            bloomResults.put(m, runBloomBuildAndProbe(dates, sales, m, k));
                        }

                        // ============================================================
                        // Run bulk + streamed approx per target range count
                        // ============================================================
                        for (int targetRangeCount : targetRangeCounts) {

                            if (targetRangeCount >= clusters) {
                                continue;
                            }

                            ApproxRunTimed bulk = runApproxRangesBuildAndProbe(
                                    dates,
                                    sales,
                                    dates.size(),
                                    targetRangeCount
                            );

                            ApproxStreamedRunTimed streamed = runApproxRangesStreamedBuildAndProbe(
                                    dates,
                                    sales,
                                    dates.size(),
                                    targetRangeCount
                            );

                            // ========================================================
                            // For each Bloom size, combine with precomputed Bloom result
                            // ========================================================
                            for (int m : bloomSizes) {
                                BloomRunTimed bl = bloomResults.get(m);

                                double speedBloomProbe = noF.probeMs / bl.probeMs;
                                double speedRangeProbe = noF.probeMs / rt.probeMs;
                                double speedApproxBulkProbe = noF.probeMs / bulk.probeMs;
                                double speedApproxStreamedProbe = noF.probeMs / streamed.probeMs;

                                double approxLookupRatioBulk =
                                        rt.hashLookups == 0 ? 0.0 : ((double) bulk.hashLookups / rt.hashLookups);

                                double approxLookupRatioStreamed =
                                        rt.hashLookups == 0 ? 0.0 : ((double) streamed.hashLookups / rt.hashLookups);

                                w.println(
                                        buildSize + "," + probeSize + "," + m + ","
                                                + clusters + "," + clusterLen + "," + gapLen + "," + targetRangeCount + ","
                                                + fmt(noF.buildMs) + "," + fmt(noF.probeMs) + ","
                                                + fmt(bl.buildMs) + "," + fmt(bl.probeMs) + ","
                                                + fmt(rt.buildMs) + "," + fmt(rt.probeMs) + ","
                                                + fmt(bulk.buildMs) + "," + fmt(bulk.probeMs) + ","
                                                + fmt(streamed.buildMs) + "," + fmt(streamed.probeMs) + ","
                                                + fmt3(speedBloomProbe) + "," + fmt3(speedRangeProbe) + ","
                                                + fmt3(speedApproxBulkProbe) + "," + fmt3(speedApproxStreamedProbe) + ","
                                                + bl.hashLookups + "," + bl.passes + "," + bl.rejects + ","
                                                + rt.hashLookups + "," + rt.passes + "," + rt.rejects + "," + rt.rangeCount + "," + rt.rangeBytes + ","
                                                + bulk.hashLookups + "," + bulk.passes + "," + bulk.rejects + ","
                                                + bulk.exactRangeCount + "," + bulk.approxRangeCount + "," + bulk.rangeBytes + ","
                                                + streamed.hashLookups + "," + streamed.passes + "," + streamed.rejects + ","
                                                + streamed.approxRangeCount + "," + streamed.rangeBytes + ","
                                                + fmt3(approxLookupRatioBulk) + "," + fmt3(approxLookupRatioStreamed)
                                );
                            }
                        }
                    }
                }
            }

            System.out.println("DONE -> " + outName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // RUNNERS
    // ============================================================

    private static TimedRun runNoFilterBuildAndProbe(List<int[]> dates, List<int[]> sales) {
        IntHashJoinOperator join = new IntHashJoinOperator(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1
        );

        long b0 = System.nanoTime();
        join.open();
        long b1 = System.nanoTime();

        long p0 = System.nanoTime();
        while (join.next() != null) { }
        long p1 = System.nanoTime();

        join.close();

        return new TimedRun(
                (b1 - b0) / 1_000_000.0,
                (p1 - p0) / 1_000_000.0
        );
    }

    private static BloomRunTimed runBloomBuildAndProbe(List<int[]> dates, List<int[]> sales, int mBits, int k) {
        IntHashJoinWithBloomFilter join = new IntHashJoinWithBloomFilter(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                mBits,
                k
        );

        long b0 = System.nanoTime();
        join.open();
        long b1 = System.nanoTime();

        long p0 = System.nanoTime();
        while (join.next() != null) { }
        long p1 = System.nanoTime();

        join.close();

        return new BloomRunTimed(
                (b1 - b0) / 1_000_000.0,
                (p1 - p0) / 1_000_000.0,
                join.getHashLookups(),
                join.getBloomPasses(),
                join.getBloomRejects()
        );
    }

    private static RangeRunTimed runExactRangesBuildAndProbe(List<int[]> dates, List<int[]> sales, int expectedBuildKeys) {
        IntHashJoinWithExactRanges join = new IntHashJoinWithExactRanges(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys
        );

        long b0 = System.nanoTime();
        join.open();
        long b1 = System.nanoTime();

        long p0 = System.nanoTime();
        while (join.next() != null) { }
        long p1 = System.nanoTime();

        join.close();

        return new RangeRunTimed(
                (b1 - b0) / 1_000_000.0,
                (p1 - p0) / 1_000_000.0,
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getRangeCount(),
                join.getRangeBytes()
        );
    }

    private static ApproxRunTimed runApproxRangesBuildAndProbe(
            List<int[]> dates,
            List<int[]> sales,
            int expectedBuildKeys,
            int targetRangeCount
    ) {
        IntHashJoinWithApproximateRanges join = new IntHashJoinWithApproximateRanges(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys,
                targetRangeCount
        );

        long b0 = System.nanoTime();
        join.open();
        long b1 = System.nanoTime();

        long p0 = System.nanoTime();
        while (join.next() != null) { }
        long p1 = System.nanoTime();

        join.close();

        return new ApproxRunTimed(
                (b1 - b0) / 1_000_000.0,
                (p1 - p0) / 1_000_000.0,
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getExactRangeCount(),
                join.getApproximateRangeCount(),
                join.getRangeBytes()
        );
    }

    private static ApproxStreamedRunTimed runApproxRangesStreamedBuildAndProbe(
            List<int[]> dates,
            List<int[]> sales,
            int expectedBuildKeys,
            int targetRangeCount
    ) {
        IntHashJoinWithApproximateRangesStreamed join = new IntHashJoinWithApproximateRangesStreamed(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys,
                targetRangeCount
        );

        long b0 = System.nanoTime();
        join.open();
        long b1 = System.nanoTime();

        long p0 = System.nanoTime();
        while (join.next() != null) { }
        long p1 = System.nanoTime();

        join.close();

        return new ApproxStreamedRunTimed(
                (b1 - b0) / 1_000_000.0,
                (p1 - p0) / 1_000_000.0,
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getApproximateRangeCount(),
                join.getRangeBytes()
        );
    }

    // ============================================================
    // DATA GENERATION
    // ============================================================

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateIntDataWithClustersAndGapNonMatchesIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            String prefix,
            int numClusters,
            int clusterLen,
            int gapLen
    ) {
        String datesPath = "data/" + prefix + "_dates_int.csv";
        String salesPath = "data/" + prefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("CLUSTER GAP-MIX data exists -> " + prefix);
            return;
        }

        System.out.println("Generating CLUSTER GAP-MIX data -> " + prefix
                + " (clusters=" + numClusters + ", clusterLen=" + clusterLen + ", gapLen=" + gapLen + ")");

        try {
            int stride = clusterLen + gapLen;
            int base = 1_000_000_000;

            int[] validIdsTmp = new int[buildSize];
            int validCount = 0;

            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,desc_code");

                outer:
                for (int c = 0; c < numClusters; c++) {
                    int start = base + c * stride;
                    int end = start + clusterLen - 1;

                    for (int id = start; id <= end; id++) {
                        if (validCount >= buildSize) break outer;

                        int year = 1980 + (id % 40);
                        int descCode = id % 1000;

                        w.println(id + "," + year + "," + descCode);
                        validIdsTmp[validCount++] = id;
                    }
                }
            }

            int[] validIds = java.util.Arrays.copyOf(validIdsTmp, validCount);

            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                for (int i = 1; i <= matchCount; i++) {
                    int id = validIds[(i - 1) % validCount];
                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                int remaining = probeSize - matchCount;

                int gapNonMatchCount = remaining / 2;
                int farNonMatchCount = remaining - gapNonMatchCount;

                int rowId = matchCount + 1;

                if (numClusters > 1 && gapLen > 0) {
                    for (int t = 0; t < gapNonMatchCount; t++) {
                        int gapIndex = t % (numClusters - 1);

                        int gapStart = base + gapIndex * stride + clusterLen;
                        int gapOffset = t % gapLen;
                        int nonMatchId = gapStart + gapOffset;

                        w.println(rowId + "," + nonMatchId + "," + (rowId % 5000) + ",10," + (rowId % 20000));
                        rowId++;
                    }
                } else {
                    farNonMatchCount += gapNonMatchCount;
                }

                int nonMatchBase = 2_000_000_000;
                for (int t = 0; t < farNonMatchCount; t++) {
                    int nonMatchId = nonMatchBase + t + 1;
                    w.println(rowId + "," + nonMatchId + "," + (rowId % 5000) + ",10," + (rowId % 20000));
                    rowId++;
                }
            }

            System.out.println("  Build rows generated (valid ids): " + validCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // HELPERS
    // ============================================================

    private static String fmt(double x) {
        return String.format(Locale.US, "%.4f", x);
    }

    private static String fmt3(double x) {
        return String.format(Locale.US, "%.3f", x);
    }

    private static final class TimedRun {
        final double buildMs;
        final double probeMs;

        TimedRun(double buildMs, double probeMs) {
            this.buildMs = buildMs;
            this.probeMs = probeMs;
        }
    }

    private static final class BloomRunTimed {
        final double buildMs;
        final double probeMs;
        final int hashLookups;
        final int passes;
        final int rejects;

        BloomRunTimed(double buildMs, double probeMs, int hashLookups, int passes, int rejects) {
            this.buildMs = buildMs;
            this.probeMs = probeMs;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
        }
    }

    private static final class RangeRunTimed {
        final double buildMs;
        final double probeMs;
        final int hashLookups;
        final int passes;
        final int rejects;
        final int rangeCount;
        final long rangeBytes;

        RangeRunTimed(double buildMs, double probeMs,
                      int hashLookups, int passes, int rejects,
                      int rangeCount, long rangeBytes) {
            this.buildMs = buildMs;
            this.probeMs = probeMs;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.rangeCount = rangeCount;
            this.rangeBytes = rangeBytes;
        }
    }

    private static final class ApproxRunTimed {
        final double buildMs;
        final double probeMs;
        final int hashLookups;
        final int passes;
        final int rejects;
        final int exactRangeCount;
        final int approxRangeCount;
        final long rangeBytes;

        ApproxRunTimed(double buildMs, double probeMs,
                       int hashLookups, int passes, int rejects,
                       int exactRangeCount, int approxRangeCount,
                       long rangeBytes) {
            this.buildMs = buildMs;
            this.probeMs = probeMs;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.exactRangeCount = exactRangeCount;
            this.approxRangeCount = approxRangeCount;
            this.rangeBytes = rangeBytes;
        }
    }

    private static final class ApproxStreamedRunTimed {
        final double buildMs;
        final double probeMs;
        final int hashLookups;
        final int passes;
        final int rejects;
        final int approxRangeCount;
        final long rangeBytes;

        ApproxStreamedRunTimed(double buildMs, double probeMs,
                               int hashLookups, int passes, int rejects,
                               int approxRangeCount, long rangeBytes) {
            this.buildMs = buildMs;
            this.probeMs = probeMs;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.approxRangeCount = approxRangeCount;
            this.rangeBytes = rangeBytes;
        }
    }
}