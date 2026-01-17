package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;

public class IntRefactoredMain {

    public static void main(String[] args) {

        System.out.println("=== STARTING INT-BASED THESIS EXPERIMENTS ===");


        runExperiment10_IntNoAvg("10");
    }

    // ============================================================
    // EXPERIMENT 10 — INT VERSION (NO AVG)
    // FIX: we time PROBE only (open/close not timed)
    // ============================================================
    private static void runExperiment10_IntNoAvg(String suffix) {

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

        try (PrintWriter writer = new PrintWriter("experiment_10_int_run" + suffix + ".csv")) {

            writer.println("build_size,probe_size,filter_size_bits,"
                    + "runtime_no_filter,runtime_bloom,speedup,"
                    + "hash_lookups,bloom_passes,bloom_rejects,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "exp10_int_" + buildSize;

                generateIntDataIfMissing(buildSize, probeSize, selectivity, prefix);

                List<int[]> dates = IntCSVReader.readCSV("data/" + prefix + "_dates_int.csv");
                List<int[]> sales = IntCSVReader.readCSV("data/" + prefix + "_sales_int.csv");

                // no filter once
                JoinMetrics noFilter;
                String noFilterStr;

                try {
                    noFilter = runNoFilterProbeOnly(dates, sales);
                    noFilterStr = String.format(Locale.US, "%.4f", noFilter.runtime);
                } catch (OutOfMemoryError oom) {
                    noFilter = null;
                    noFilterStr = "OOM";
                }

                for (int m : bloomSizes) {

                    JoinMetrics bloom;
                    String bloomStr;

                    try {
                        bloom = runBloomProbeOnly(dates, sales, m, k);
                        bloomStr = String.format(Locale.US, "%.4f", bloom.runtime);
                    } catch (OutOfMemoryError oom) {
                        bloom = null;
                        bloomStr = "OOM";
                    }

                    String speedupStr = "NA";
                    if (!noFilterStr.equals("OOM") && !bloomStr.equals("OOM")) {
                        speedupStr = String.format(Locale.US, "%.3f",
                                noFilter.runtime / bloom.runtime);
                    }

                    writer.println(buildSize + "," + probeSize + "," + m + ","
                            + noFilterStr + ","
                            + bloomStr + ","
                            + speedupStr + ","
                            + (bloom != null ? bloom.hashLookups : -1) + ","
                            + (bloom != null ? bloom.bloomPasses : -1) + ","
                            + (bloom != null ? bloom.bloomRejects : -1) + ","
                            + (bloom != null ? bloom.actualMatches : -1)
                    );
                }
            }

            System.out.println("INT Experiment 10 DONE → experiment_10_int_run" + suffix + ".csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // PROBE-ONLY TIMING HELPERS
    // ============================================================
    private static JoinMetrics runNoFilterProbeOnly(List<int[]> dates, List<int[]> sales) {

        IntHashJoinOperator join = new IntHashJoinOperator(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1
        );

        join.open(); // build not timed

        long start = System.nanoTime();
        int matches = 0;
        while (join.next() != null) matches++;
        long end = System.nanoTime();

        join.close();

        return new JoinMetrics(
                (end - start) / 1_000_000.0,
                join.getHashLookups(),
                0,
                0,
                matches
        );
    }

    private static JoinMetrics runBloomProbeOnly(
            List<int[]> dates,
            List<int[]> sales,
            int filterSize,
            int k
    ) {

        IntHashJoinWithBloomFilter join = new IntHashJoinWithBloomFilter(
                new IntScanOperator(dates),
                new IntScanOperator(sales),
                0, 1,
                filterSize,
                k
        );

        join.open(); // build+bloom insert not timed

        long start = System.nanoTime();
        int matches = 0;
        while (join.next() != null) matches++;
        long end = System.nanoTime();

        join.close();

        return new JoinMetrics(
                (end - start) / 1_000_000.0,
                join.getHashLookups(),
                join.getBloomPasses(),
                join.getBloomRejects(),
                matches
        );
    }

    // ============================================================
    // INT DATASET GENERATION (LOCAL)
    // ============================================================
    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateIntDataIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            String prefix
    ) {

        String datesPath = "data/" + prefix + "_dates_int.csv";
        String salesPath = "data/" + prefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("INT data already exists → " + prefix);
            return;
        }

        System.out.println("Generating INT data: " + prefix);

        try {
            // dates: id,year,desc_code
            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,desc_code");
                for (int i = 1; i <= buildSize; i++) {
                    int year = 1980 + (i % 40);
                    int descCode = i % 1000;
                    w.println(i + "," + year + "," + descCode);
                }
            }

            // sales: sale_id,date_id,product_code,price,customer_code
            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product_code,price,customer_code");

                int matchCount = (int) (probeSize * selectivity);

                for (int i = 1; i <= matchCount; i++) {
                    int id = ((i - 1) % buildSize) + 1;
                    w.println(i + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
                }

                for (int i = matchCount + 1; i <= probeSize; i++) {
                    int nonMatchId = buildSize + 1000 + i;
                    w.println(i + "," + nonMatchId + "," + (i % 5000) + ",10," + (i % 20000));
                }
            }

            System.out.println("  Done → " + prefix);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // METRICS
    // ============================================================
    private static class JoinMetrics {
        double runtime;
        int hashLookups;
        int bloomPasses;
        int bloomRejects;
        int actualMatches;

        JoinMetrics(double runtime, int hashLookups,
                    int bloomPasses, int bloomRejects,
                    int actualMatches) {

            this.runtime = runtime;
            this.hashLookups = hashLookups;
            this.bloomPasses = bloomPasses;
            this.bloomRejects = bloomRejects;
            this.actualMatches = actualMatches;
        }
    }
}
