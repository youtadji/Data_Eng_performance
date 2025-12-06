package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.CSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;

public class RefactoredMain {

    public static void main(String[] args) {

        System.out.println("=== STARTING THESIS EXPERIMENTS ===");

        // datasets
        //generateAllDatasets();

        //  run one  at a time.

        //runExperiment10_BigBuildNoAvg("1");
        //runExperiment10_BigBuildNoAvg("2");
        //runExperiment10_BigBuildNoAvg("3");
        //runExperiment10_BigBuildNoAvg("4");
        //runExperiment10_BigBuildNoAvg("5");
        //runExperiment10_BigBuildNoAvg("6");
        //runExperiment10_BigBuildNoAvg("7");
        //runExperiment10_BigBuildNoAvg("8");
        //runExperiment10_BigBuildNoAvg("9");
        runExperiment10_BigBuildNoAvg("10");
    }

    // ============================================================
    // DATASET GENERATION
    // ============================================================
    private static void generateAllDatasets() {

        System.out.println("\n=== GENERATING ALL DATASETS (ONCE) ===\n");

        int probeSize = 10_000_000;
        double selectivity = 0.05;

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000      // 5M & 10M removed to avoid OOM
        };

        // Datasets for Experiment 1 + 4 (build_*) and Experiment 3 (optk_*)
        for (int buildSize : buildSizes) {
            generateTestDataIfMissing(buildSize, probeSize, selectivity, "build_" + buildSize);
            generateTestDataIfMissing(buildSize, probeSize, selectivity, "optk_" + buildSize);
        }

        // Dataset for Experiment 2 (filter size, fixed build size)
        generateTestDataIfMissing(1_000_000, probeSize, selectivity, "filter_fixed_1M_build");

        System.out.println("\n=== ALL DATASETS READY ===\n");
    }

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static void generateTestDataIfMissing(
            int buildSize, int probeSize,
            double selectivity, String prefix
    ) {

        String datesPath = "data/" + prefix + "_dates.csv";
        String salesPath = "data/" + prefix + "_sales.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("Data already exists → " + prefix);
            return;
        }

        System.out.println("Generating data: " + prefix);

        try {
            // Build side
            try (PrintWriter w = new PrintWriter(datesPath)) {
                w.println("id,year,description");
                for (int i = 1; i <= buildSize; i++) {
                    w.println(i + "," + (1980 + i % 40) + ",desc");
                }
            }

            // Probe side
            try (PrintWriter w = new PrintWriter(salesPath)) {
                w.println("sale_id,date_id,product,price,customer");

                int matchCount = (int) (probeSize * selectivity);

                // matching keys
                for (int i = 1; i <= matchCount; i++) {
                    int id = ((i - 1) % buildSize) + 1;
                    w.println(i + "," + id + ",X,10,C");
                }

                // non-matching keys
                for (int i = matchCount + 1; i <= probeSize; i++) {
                    w.println(i + "," + (buildSize + 1000 + i) + ",X,10,C");
                }
            }

            System.out.println("  Done → " + prefix);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ============================================================
// EXPERIMENT 10 — BIG BUILD SIZES, MULTIPLE FILTER SIZES, NO AVERAGING
// ============================================================
    private static void runExperiment10_BigBuildNoAvg(String suffix) {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000,
        };


        int[] bloomSizes = {
                16_000_000,
                160_000_000,
                1_600_000_000
        };

        int probeSize = 10_000_000;
        double selectivity = 0.05;
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_10_run" + suffix + ".csv")) {

            writer.println("build_size,probe_size,filter_size_bits,"
                    + "runtime_no_filter,runtime_bloom,speedup,"
                    + "hash_lookups,bloom_passes,bloom_rejects,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "exp10_" + buildSize;
                generateTestDataIfMissing(buildSize, probeSize, selectivity, prefix);

                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // NO FILTER ONCE
                JoinMetrics noFilter = null;
                String noFilterStr;

                try {
                    noFilter = runNoFilterJoin(dates, sales);
                    noFilterStr = String.format(Locale.US, "%.4f", noFilter.runtime);
                } catch (OutOfMemoryError oom) {
                    noFilterStr = "OOM";
                }

                for (int m : bloomSizes) {

                    JoinMetrics bloom = null;
                    String bloomStr;

                    try {
                        bloom = runBloomJoin(dates, sales, m, k);
                        bloomStr = String.format(Locale.US, "%.4f", bloom.runtime);
                    } catch (OutOfMemoryError oom) {
                        bloomStr = "OOM";
                    }

                    String speedupStr = "NA";
                    if (!noFilterStr.equals("OOM") && !bloomStr.equals("OOM")) {
                        speedupStr = String.format(Locale.US,
                                "%.3f", noFilter.runtime / bloom.runtime);
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

            System.out.println("Experiment 10 DONE → experiment_10_run" + suffix + ".csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }







    // ============================================================
    // CORE JOIN RUNS (single run)
    // ============================================================
    private static JoinMetrics runNoFilterJoin(List<String[]> dates, List<String[]> sales) {

        HashJoinOperator join = new HashJoinOperator(
                new ScanOperator(dates),
                new ScanOperator(sales),
                0, 1
        );

        long start = System.nanoTime();
        join.open();

        int matches = 0;
        while (join.next() != null) {
            matches++;
        }

        long end = System.nanoTime();

        return new JoinMetrics(
                (end - start) / 1_000_000.0,
                join.getHashLookups(),
                0,
                0,
                matches
        );
    }

    private static JoinMetrics runBloomJoin(
            List<String[]> dates,
            List<String[]> sales,
            int filterSize,
            int numHashFunctions
    ) {

        HashJoinWithBloomFilter join = new HashJoinWithBloomFilter(
                new ScanOperator(dates),
                new ScanOperator(sales),
                0, 1,
                filterSize,
                numHashFunctions
        );

        long start = System.nanoTime();
        join.open();

        int matches = 0;
        while (join.next() != null) {
            matches++;
        }

        long end = System.nanoTime();

        return new JoinMetrics(
                (end - start) / 1_000_000.0,
                join.getHashLookups(),
                join.getBloomPasses(),
                join.getBloomRejects(),
                matches
        );
    }

    // ============================================================
    // AVERAGING HELPERS
    // ============================================================
    private static JoinMetrics averageNoFilter(
            List<String[]> dates, List<String[]> sales, int runs
    ) {
        double totalRuntime = 0.0;
        long totalLookups = 0;
        int matches = 0;

        for (int i = 0; i < runs; i++) {
            System.out.println("  NoFilter run " + (i + 1) + "/" + runs);
            JoinMetrics m = runNoFilterJoin(dates, sales);
            totalRuntime += m.runtime;
            totalLookups += m.hashLookups;
            matches = m.actualMatches;
        }

        return new JoinMetrics(
                totalRuntime / runs,
                (int) (totalLookups / runs),
                0,
                0,
                matches
        );
    }

    private static JoinMetrics averageBloom(
            List<String[]> dates,
            List<String[]> sales,
            int filterSize,
            int k,
            int runs
    ) {
        double totalRuntime = 0.0;
        long totalLookups = 0;
        long totalPasses = 0;
        long totalRejects = 0;
        int matches = 0;

        for (int i = 0; i < runs; i++) {
            System.out.println("  Bloom run " + (i + 1) + "/" + runs + " (m=" + filterSize + ",k=" + k + ")");
            JoinMetrics m = runBloomJoin(dates, sales, filterSize, k);
            totalRuntime += m.runtime;
            totalLookups += m.hashLookups;
            totalPasses += m.bloomPasses;
            totalRejects += m.bloomRejects;
            matches = m.actualMatches;
        }

        return new JoinMetrics(
                totalRuntime / runs,
                (int) (totalLookups / runs),
                (int) (totalPasses / runs),
                (int) (totalRejects / runs),
                matches
        );
    }

    // ============================================================
    // OPTIMAL k (CAPPED AT 8)
    // ============================================================
    private static int computeOptimalK(int filterSizeBits, int buildSize) {
        double k = (filterSizeBits / (double) buildSize) * Math.log(2);
        int optimal = (int) Math.round(k);
        if (optimal < 1) optimal = 1;
        if (optimal > 8) optimal = 8;  // cap to avoid insane CPU cost
        return optimal;
    }

    // ============================================================
    // METRICS CLASS
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


