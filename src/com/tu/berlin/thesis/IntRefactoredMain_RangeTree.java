package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;

public class IntRefactoredMain_RangeTree {

    public static void main(String[] args) {
        System.out.println("=== EXP10 INT: NoFilter vs Bloom vs RangeTree ===");
        runExperiment10_Int("1");
    }

    // ============================================================
    // Experiment 10 (INT) — compares:
    //  - no filter (baseline)
    //  - bloom prefilter
    //  - exact range tree (B+tree over exact ranges)
    // Probe-only timing for fairness.
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

        try (PrintWriter w = new PrintWriter("experiment_10_int_rangetree_run" + suffix + ".csv")) {

            w.println("build_size,probe_size,filter_size_bits,"
                    + "probe_ms_no_filter,probe_ms_bloom,probe_ms_rangetree,"
                    + "speedup_bloom,speedup_rangetree,"
                    + "hash_lookups_bloom,bloom_passes,bloom_rejects,"
                    + "hash_lookups_range,range_passes,range_rejects,range_count,range_bytes");

            for (int buildSize : buildSizes) {

                //String prefix = "exp10_int_" + buildSize;
                int gapEvery = 100; // you can tune later
                String prefix = "exp10_gap" + gapEvery + "_" + buildSize;


                // generate if missing (keeps old files untouched)
                generateIntDataWithGapsIfMissing(buildSize, probeSize, selectivity, prefix, gapEvery);


                // load datasets
                List<int[]> dates = IntCSVReader.readCSV("data/" + prefix + "_dates_int.csv");
                List<int[]> sales = IntCSVReader.readCSV("data/" + prefix + "_sales_int.csv");

                // baseline: no filter
                JoinMs noF = runNoFilterProbeOnly(dates, sales);



               // RangeRun rt = runRangeTreeProbeOnly(dates, sales, buildSize); //eya here i was only with one range

                RangeRun rt = runRangeTreeProbeOnly(dates, sales, dates.size());

                for (int m : bloomSizes) {
                    // bloom
                    BloomRun bl = runBloomProbeOnly(dates, sales, m, k);

                    // range tree (exact)
                    //RangeRun rt = runRangeTreeProbeOnly(dates, sales, buildSize);

                    double speedBloom = noF.ms / bl.ms;
                    double speedRange = noF.ms / rt.ms;

                    w.println(buildSize + "," + probeSize + "," + m + ","
                            + fmt(noF.ms) + "," + fmt(bl.ms) + "," + fmt(rt.ms) + ","
                            + fmt3(speedBloom) + "," + fmt3(speedRange) + ","
                            + bl.hashLookups + "," + bl.passes + "," + bl.rejects + ","
                            + rt.hashLookups + "," + rt.passes + "," + rt.rejects + ","
                            + rt.rangeCount + "," + rt.rangeBytes
                    );
                }
            }

            System.out.println("DONE -> experiment_10_int_rangetree_run" + suffix + ".csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // Probe-only runners
    // ============================================================

    private static JoinMs runNoFilterProbeOnly(List<int[]> dates, List<int[]> sales) {
        IntHashJoinOperator join = new IntHashJoinOperator(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1
        );

        join.open(); // build not timed

        long t0 = System.nanoTime();
        while (join.next() != null) { }
        long t1 = System.nanoTime();

        join.close();

        return new JoinMs((t1 - t0) / 1_000_000.0);
    }

    private static BloomRun runBloomProbeOnly(List<int[]> dates, List<int[]> sales, int mBits, int k) {
        IntHashJoinWithBloomFilter join = new IntHashJoinWithBloomFilter(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                mBits,
                k
        );

        join.open(); // build not timed

        long t0 = System.nanoTime();
        while (join.next() != null) { }
        long t1 = System.nanoTime();

        join.close();

        return new BloomRun(
                (t1 - t0) / 1_000_000.0,
                join.getHashLookups(),
                join.getBloomPasses(),
                join.getBloomRejects()
        );
    }

    private static RangeRun runRangeTreeProbeOnly(List<int[]> dates, List<int[]> sales, int expectedBuildKeys) {
        IntHashJoinWithRangeTree join = new IntHashJoinWithRangeTree(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                expectedBuildKeys
        );

        join.open(); // build (extract ranges + build tree) not timed

        long t0 = System.nanoTime();
        while (join.next() != null) { }
        long t1 = System.nanoTime();

        join.close();

        return new RangeRun(
                (t1 - t0) / 1_000_000.0,
                join.getHashLookups(),
                join.getRangePasses(),
                join.getRangeRejects(),
                join.getRangeCount(),
                join.getRangeBytes()
        );
    }

    // ============================================================
    // INT DATASET GENERATION (same as your int main)
    // ============================================================

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateIntDataIfMissing(int buildSize, int probeSize, double selectivity, String prefix) {

        String datesPath = "data/" + prefix + "_dates_int.csv";
        String salesPath = "data/" + prefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("INT data exists -> " + prefix);
            return;
        }

        System.out.println("Generating INT data -> " + prefix);

        try {
            // Build side: id,year,desc_code
            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,desc_code");
                for (int i = 1; i <= buildSize; i++) {
                    int year = 1980 + (i % 40);
                    int descCode = i % 1000;
                    w.println(i + "," + year + "," + descCode);
                }
            }

            // Probe side: sale_id,date_id,product_code,price,customer_code
            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                // matching keys
                for (int i = 1; i <= matchCount; i++) {
                    int id = ((i - 1) % buildSize) + 1;
                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                // non-matching keys
                for (int i = matchCount + 1; i <= probeSize; i++) {
                    int nonMatchId = buildSize + 1000 + i;
                    w.println(i + "," + nonMatchId + "," + (i % 5000) + ",10," + (i % 20000));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void generateIntDataWithGapsIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            String prefix,
            int gapEvery
    ) {
        String datesPath = "data/" + prefix + "_dates_int.csv";
        String salesPath = "data/" + prefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("GAP data exists -> " + prefix);
            return;
        }

        System.out.println("Generating GAP data -> " + prefix + " (gapEvery=" + gapEvery + ")");

        try {
            // -----------------------
            // BUILD SIDE with gaps
            // -----------------------
            // keep ids 1..buildSize EXCEPT ids divisible by gapEvery
            int validCount = 0;

            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,desc_code");

                for (int id = 1; id <= buildSize; id++) {
                    if (gapEvery > 0 && id % gapEvery == 0) continue; // gap

                    int year = 1980 + (id % 40);
                    int descCode = id % 1000;
                    w.println(id + "," + year + "," + descCode);
                    validCount++;
                }
            }

            // -----------------------
            // PROBE SIDE
            // -----------------------
            // matching keys must only be from VALID ids (non-missing)
            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                for (int i = 1; i <= matchCount; i++) {
                    int t = ((i - 1) % validCount) + 1; // 1..validCount
                    int id = mapToActualIdSkippingEveryK(t, gapEvery);

                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                for (int i = matchCount + 1; i <= probeSize; i++) {
                    int nonMatchId = buildSize + 1000 + i;
                    w.println(i + "," + nonMatchId + "," + (i % 5000) + ",10," + (i % 20000));
                }
            }

            System.out.println("  Build rows kept (valid ids): " + validCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Maps t = 1..validCount to the real id when skipping every k-th id (k,2k,3k,...).
     * Example k=100:
     *  t=1 -> 1
     *  t=99 -> 99
     *  t=100 -> 101   (since 100 is skipped)
     */
    private static int mapToActualIdSkippingEveryK(int t, int k) {
        if (k <= 0) return t;

        int perBlock = k - 1;                 // kept ids per block of k
        int skippedBefore = (t - 1) / perBlock; // how many skipped ids before t
        return t + skippedBefore;
    }

    // ============================================================
    // Helpers / metrics structs
    // ============================================================

    private static String fmt(double x) { return String.format(Locale.US, "%.4f", x); }
    private static String fmt3(double x) { return String.format(Locale.US, "%.3f", x); }

    private static final class JoinMs {
        final double ms;
        JoinMs(double ms) { this.ms = ms; }
    }

    private static final class BloomRun {
        final double ms;
        final int hashLookups;
        final int passes;
        final int rejects;

        BloomRun(double ms, int hashLookups, int passes, int rejects) {
            this.ms = ms;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
        }
    }

    private static final class RangeRun {
        final double ms;
        final int hashLookups;
        final int passes;
        final int rejects;
        final int rangeCount;
        final long rangeBytes;

        RangeRun(double ms, int hashLookups, int passes, int rejects, int rangeCount, long rangeBytes) {
            this.ms = ms;
            this.hashLookups = hashLookups;
            this.passes = passes;
            this.rejects = rejects;
            this.rangeCount = rangeCount;
            this.rangeBytes = rangeBytes;
        }
    }
}