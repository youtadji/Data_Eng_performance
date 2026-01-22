package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;

public class IntRefactoredMainExactRangeTree {

    public static void main(String[] args) {
        System.out.println("=== EXP10 INT: NoFilter vs Bloom vs ExactRanges (Build+Probe) [CLUSTERS] ===");
        runExperiment10_Int("3");
    }

    // ============================================================
    // Experiment 10 (INT) — compares:
    //  - no filter (baseline)
    //  - bloom prefilter
    //  - exact ranges (2 arrays)
    //
    // Measures BUILD + PROBE time for each join.
    // Data characteristic: FEW RANGES via CLUSTERS (Option A).
    //
    // IMPORTANT FIX:
    // clusterLen is computed per buildSize & clusters so that we
    // actually generate "clusters" many ranges even for small buildSize.
    // ============================================================
    private static void runExperiment10_Int(String suffix) {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
        };

        int[] bloomSizes = {
                16_000_000,
                160_000_000,
                1_600_000_000
        };

        int probeSize = 10_000_000;
        double selectivity = 0.05;
        int k = 2;

        // ============================
        // CLUSTER knobs (few ranges)
        // ============================
        int[] clusterCounts = {1, 2, 4, 8, 16};
        int gapLen = 5_000_000; // big gaps between clusters

        try (PrintWriter w = new PrintWriter(
                "experiment_10_int_exactranges_buildprobe_clusters_run" + suffix + ".csv")) {

            w.println("build_size,probe_size,filter_size_bits,clusters,cluster_len,gap_len,"
                    + "build_ms_no_filter,probe_ms_no_filter,"
                    + "build_ms_bloom,probe_ms_bloom,"
                    + "build_ms_exactranges,probe_ms_exactranges,"
                    + "speedup_probe_bloom,speedup_probe_exactranges,"
                    + "hash_lookups_bloom,bloom_passes,bloom_rejects,"
                    + "hash_lookups_range,range_passes,range_rejects,range_count,range_bytes");

            for (int buildSize : buildSizes) {

                for (int clusters : clusterCounts) {

                    // ----------------------------
                    // FIX: compute clusterLen so we actually get "clusters" ranges
                    // ----------------------------
                    int clusterLen = (buildSize + clusters - 1) / clusters; // ceil(buildSize/clusters)

                    String prefix = "exp10_cluC" + clusters
                            + "_L" + clusterLen
                            + "_G" + gapLen
                            + "_" + buildSize;

                    // Generate CLUSTER data if missing
                    generateIntDataWithClustersIfMissing(
                            buildSize, probeSize, selectivity,
                            prefix, clusters, clusterLen, gapLen
                    );

                    // Load datasets
                    List<int[]> dates = IntCSVReader.readCSV("data/" + prefix + "_dates_int.csv");
                    List<int[]> sales = IntCSVReader.readCSV("data/" + prefix + "_sales_int.csv");

                    // Baseline: no filter (build+probe)
                    TimedRun noF = runNoFilterBuildAndProbe(dates, sales);

                    // Exact ranges (2 arrays)
                    RangeRunTimed rt = runExactRangesBuildAndProbe(dates, sales, dates.size());

                    // Bloom depends on m -> run per m
                    for (int m : bloomSizes) {
                        BloomRunTimed bl = runBloomBuildAndProbe(dates, sales, m, k);

                        // Speedups based on PROBE time
                        double speedBloomProbe = noF.probeMs / bl.probeMs;
                        double speedRangeProbe = noF.probeMs / rt.probeMs;

                        w.println(buildSize + "," + probeSize + "," + m + ","
                                + clusters + "," + clusterLen + "," + gapLen + ","
                                + fmt(noF.buildMs) + "," + fmt(noF.probeMs) + ","
                                + fmt(bl.buildMs) + "," + fmt(bl.probeMs) + ","
                                + fmt(rt.buildMs) + "," + fmt(rt.probeMs) + ","
                                + fmt3(speedBloomProbe) + "," + fmt3(speedRangeProbe) + ","
                                + bl.hashLookups + "," + bl.passes + "," + bl.rejects + ","
                                + rt.hashLookups + "," + rt.passes + "," + rt.rejects + ","
                                + rt.rangeCount + "," + rt.rangeBytes
                        );
                    }
                }
            }

            System.out.println("DONE -> experiment_10_int_exactranges_buildprobe_clusters_run" + suffix + ".csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // BUILD + PROBE runners
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

        return new TimedRun((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0);
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

    // ============================================================
    // DATASET GENERATION: CLUSTERS (few ranges)
    // ============================================================

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateIntDataWithClustersIfMissing(
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
            System.out.println("CLUSTER data exists -> " + prefix);
            return;
        }

        System.out.println("Generating CLUSTER data -> " + prefix
                + " (clusters=" + numClusters + ", clusterLen=" + clusterLen + ", gapLen=" + gapLen + ")");

        try {
            int stride = clusterLen + gapLen;

            int[] validIdsTmp = new int[buildSize];
            int validCount = 0;

            // BUILD: generate up to buildSize keys across numClusters clusters
            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,desc_code");

                int base = 1_000_000_000; // away from 1..N

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

            // PROBE
            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                for (int i = 1; i <= matchCount; i++) {
                    int id = validIds[(i - 1) % validCount];
                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                int nonMatchBase = 2_000_000_000;
                for (int i = matchCount + 1; i <= probeSize; i++) {
                    int nonMatchId = nonMatchBase + i;
                    w.println(i + "," + nonMatchId + "," + (i % 5000) + ",10," + (i % 20000));
                }
            }

            System.out.println("  Build rows generated (valid ids): " + validCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // Helpers / metrics structs
    // ============================================================

    private static String fmt(double x) { return String.format(Locale.US, "%.4f", x); }
    private static String fmt3(double x) { return String.format(Locale.US, "%.3f", x); }

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
}
