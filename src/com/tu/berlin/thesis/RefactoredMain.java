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
        //runExperimentBuildSize();      // Experiment 1
        //runExperimentFilterSize();   // Experiment 2
        //runExperimentOptimalK();     // Experiment 3
        //runExperimentSweetSpot();    // Experiment 4
        //runExperimentBuildSizeLowSelectivity(); //exp5
        //runExperimentSelectivitySweep();    // Experiment 6
        //runExperimentReducedBloomSize(); //Exp7
        //runExperiment9_BigBuildNoAvg(); //Exp9
        //runExperiment10_BigBuildNoAvg();
        runExperiment10_BigBuildNoAvg("1");

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
    // EXPERIMENT 1 — RUNTIME vs BUILD SIZE (AVERAGED)
    // ============================================================
    private static void runExperimentBuildSize() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
        };

        int probeSize = 10_000_000;
        int bloomFilterSize = 16_000_000; // 16M bits
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_build_size.csv")) {

            writer.println("build_size,probe_size,runtime_no_filter_avg,runtime_bloom_avg,"
                    + "speedup_avg,hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "build_" + buildSize;

                System.out.println("\n=== Experiment 1: Build Size = " + buildSize + " ===");

                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Runs per size (Option B)
                int runs;
                if (buildSize <= 330_000) {
                    runs = 10;
                } else if (buildSize <= 1_000_000) {
                    runs = 7;
                } else {
                    runs = 3;  // 3M
                }

                // NoFilter only for buildSize <= 1M (to avoid OOM)
                boolean runNoFilter = buildSize <= 1_000_000;

                JoinMetrics noFilterAvg = null;
                if (runNoFilter) {
                    noFilterAvg = averageNoFilter(dates, sales, runs);
                } else {
                    System.out.println("Skipping NoFilter (OOM risk) for build size " + buildSize);
                }

                JoinMetrics bloomAvg = averageBloom(dates, sales, bloomFilterSize, k, runs);

                String noFilterStr = runNoFilter
                        ? String.format(Locale.US, "%.4f", noFilterAvg.runtime)
                        : "OOM";

                String speedupStr = runNoFilter
                        ? String.format(Locale.US, "%.3f", noFilterAvg.runtime / bloomAvg.runtime)
                        : "NA";

                String line = buildSize + "," + probeSize + "," +
                        noFilterStr + "," +
                        String.format(Locale.US, "%.4f", bloomAvg.runtime) + "," +
                        speedupStr + "," +
                        bloomAvg.hashLookups + "," +
                        bloomAvg.bloomPasses + "," +
                        bloomAvg.bloomRejects + "," +
                        bloomAvg.actualMatches;

                writer.println(line);
            }

            System.out.println("\nExperiment 1 DONE → experiment_build_size.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // EXPERIMENT 2 — RUNTIME vs FILTER SIZE (AVERAGED)
    // ============================================================
    private static void runExperimentFilterSize() {

        int buildSize = 1_000_000;
        int probeSize = 10_000_000;

        int[] filterSizes = {
                64_000_000,
                128_000_000,
                1_280_000_000,
                2_147_483_646
        };

        String prefix = "filter_fixed_1M_build";

        try (PrintWriter writer = new PrintWriter("experiment_filter_size.csv")) {

            writer.println("filter_size,runtime_ms_avg,hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
            List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

            for (int m : filterSizes) {

                System.out.println("\n=== Experiment 2: Filter Size = " + m + " ===");

                int runs = (m <= 128_000_000) ? 10 : 5;  // Option B averaging

                JoinMetrics bloomAvg = averageBloom(dates, sales, m, 2, runs);

                writer.println(String.format(Locale.US,
                        "%d,%.4f,%d,%d,%d,%d",
                        m,
                        bloomAvg.runtime,
                        bloomAvg.hashLookups,
                        bloomAvg.bloomPasses,
                        bloomAvg.bloomRejects,
                        bloomAvg.actualMatches
                ));
            }

            System.out.println("\nExperiment 2 DONE → experiment_filter_size.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // EXPERIMENT 3 — OPTIMAL k (AVERAGED)
    // ============================================================
    private static void runExperimentOptimalK() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
        };

        int probeSize = 10_000_000;
        int filterSize = 128_000_000; // same as sweet spot

        try (PrintWriter writer = new PrintWriter("experiment_optimal_k.csv")) {

            writer.println("build_size,probe_size,runtime_no_filter_avg,"
                    + "runtime_bloom_k2_avg,runtime_bloom_optimal_k_avg,"
                    + "k_optimal,hash_lookups_k2_avg,hash_lookups_opt_avg,"
                    + "bloom_passes_k2_avg,bloom_passes_opt_avg,"
                    + "bloom_rejects_k2_avg,bloom_rejects_opt_avg");

            for (int buildSize : buildSizes) {

                String prefix = "optk_" + buildSize;

                System.out.println("\n=== Experiment 3: Build Size = " + buildSize + " ===");

                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                int runs;
                if (buildSize <= 330_000) {
                    runs = 10;
                } else if (buildSize <= 1_000_000) {
                    runs = 7;
                } else {
                    runs = 3;
                }

                boolean runNoFilter = buildSize <= 1_000_000;

                JoinMetrics noFilterAvg = null;
                if (runNoFilter) {
                    noFilterAvg = averageNoFilter(dates, sales, runs);
                } else {
                    System.out.println("Skipping NoFilter (OOM risk) for build size " + buildSize);
                }

                JoinMetrics bloomK2Avg = averageBloom(dates, sales, filterSize, 2, runs);

                int kOptimal = computeOptimalK(filterSize, buildSize);
                JoinMetrics bloomOptAvg = averageBloom(dates, sales, filterSize, kOptimal, runs);

                String noFilterStr = runNoFilter
                        ? String.format(Locale.US, "%.4f", noFilterAvg.runtime)
                        : "OOM";

                String line = buildSize + "," +
                        probeSize + "," +
                        noFilterStr + "," +
                        String.format(Locale.US, "%.4f", bloomK2Avg.runtime) + "," +
                        String.format(Locale.US, "%.4f", bloomOptAvg.runtime) + "," +
                        kOptimal + "," +
                        bloomK2Avg.hashLookups + "," +
                        bloomOptAvg.hashLookups + "," +
                        bloomK2Avg.bloomPasses + "," +
                        bloomOptAvg.bloomPasses + "," +
                        bloomK2Avg.bloomRejects + "," +
                        bloomOptAvg.bloomRejects;

                writer.println(line);
            }

            System.out.println("\nExperiment 3 DONE → experiment_optimal_k.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // ============================================================
// EXPERIMENT 4 — SWEET SPOT BLOOM FILTER (128M bits, k=2)
// WITH NO-FILTER BASELINE + AVERAGING + OOM SKIP
// ============================================================
    private static void runExperimentSweetSpot() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
        };

        int probeSize = 10_000_000;
        int filterSize = 128_000_000; // sweet spot
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_sweet_spot.csv")) {

            writer.println("build_size,probe_size,"
                    + "runtime_no_filter_avg,runtime_bloom_avg,"
                    + "speedup_avg,"
                    + "hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "build_" + buildSize;

                System.out.println("\n=== Experiment 4 (Sweet Spot): Build Size = " + buildSize + " ===");

                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Runs per size (Option B)
                int runs;
                if (buildSize <= 330_000) {
                    runs = 10;
                } else if (buildSize <= 1_000_000) {
                    runs = 7;
                } else {
                    runs = 3;  // <= 3M
                }

                // NoFilter baseline allowed only for <=1M (avoid OOM)
                boolean runNoFilter = buildSize <= 1_000_000;

                JoinMetrics noFilterAvg = null;
                if (runNoFilter) {
                    noFilterAvg = averageNoFilter(dates, sales, runs);
                } else {
                    System.out.println("Skipping NoFilter (OOM risk)");
                }

                JoinMetrics bloomAvg = averageBloom(dates, sales, filterSize, k, runs);

                String noFilterStr = runNoFilter
                        ? String.format(Locale.US, "%.4f", noFilterAvg.runtime)
                        : "OOM";

                String speedupStr = runNoFilter
                        ? String.format(Locale.US, "%.3f", noFilterAvg.runtime / bloomAvg.runtime)
                        : "NA";

                writer.println(String.format(Locale.US,
                        "%d,%d,%s,%.4f,%s,%d,%d,%d,%d",
                        buildSize,
                        probeSize,
                        noFilterStr,
                        bloomAvg.runtime,
                        speedupStr,
                        bloomAvg.hashLookups,
                        bloomAvg.bloomPasses,
                        bloomAvg.bloomRejects,
                        bloomAvg.actualMatches
                ));
            }

            System.out.println("\nExperiment 4 DONE → experiment_sweet_spot.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
// EXPERIMENT 5 — RUNTIME vs BUILD SIZE with selectivity = 0.01
// (same as Experiment 1 but lower selectivity)
// ============================================================
    private static void runExperimentBuildSizeLowSelectivity() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
        };

        int probeSize = 10_000_000;
        double selectivity = 0.01;          // 1% matches (was 5% before)
        int bloomFilterSize = 16_000_000;   // same as Experiment 1
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_build_size_sel001.csv")) {

            writer.println("build_size,probe_size,"
                    + "runtime_no_filter_avg,runtime_bloom_avg,speedup_avg,"
                    + "hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "sel001_build_" + buildSize;

                System.out.println("\n=== Experiment 5 (sel=0.01): Build Size = " + buildSize + " ===");

                // Generate data for this build size & selectivity if missing
                generateTestDataIfMissing(buildSize, probeSize, selectivity, prefix);

                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Runs per size (same Option B logic)
                int runs;
                if (buildSize <= 330_000) {
                    runs = 10;
                } else if (buildSize <= 1_000_000) {
                    runs = 7;
                } else {
                    runs = 3;  // 3M
                }

                // Baseline: NoFilter only up to 1M (avoid OOM at 3M)
                boolean runNoFilter = buildSize <= 1_000_000;

                JoinMetrics noFilterAvg = null;
                if (runNoFilter) {
                    noFilterAvg = averageNoFilter(dates, sales, runs);
                } else {
                    System.out.println("Skipping NoFilter (OOM risk) for build size " + buildSize);
                }

                // Bloom with 16M bits and k=2
                JoinMetrics bloomAvg = averageBloom(dates, sales, bloomFilterSize, k, runs);

                String noFilterStr = runNoFilter
                        ? String.format(Locale.US, "%.4f", noFilterAvg.runtime)
                        : "OOM";

                String speedupStr = runNoFilter
                        ? String.format(Locale.US, "%.3f", noFilterAvg.runtime / bloomAvg.runtime)
                        : "NA";

                writer.println(String.format(Locale.US,
                        "%d,%d,%s,%.4f,%s,%d,%d,%d,%d",
                        buildSize,
                        probeSize,
                        noFilterStr,
                        bloomAvg.runtime,
                        speedupStr,
                        bloomAvg.hashLookups,
                        bloomAvg.bloomPasses,
                        bloomAvg.bloomRejects,
                        bloomAvg.actualMatches
                ));
            }

            System.out.println("\nExperiment 5 DONE → experiment_build_size_sel001.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // ============================================================
// EXPERIMENT 6 — SPEEDUP vs SELECTIVITY
// ============================================================
    private static void runExperimentSelectivitySweep() {

        // The selectivities we test:
        double[] selectivities = {
                0.05,      // 5%
                0.01,      // 1%
                0.001,     // 0.1%
                0.0001     // 0.01%  (Bloom will shine here)
        };

        int buildSize = 1_000_000;          // fixed build size
        int probeSize = 10_000_000;         // fixed probe size

        int filterSize = 16_000_000;        // good mid-size Bloom filter (2MB)
        int k = 2;                          // good for speed

        try (PrintWriter writer = new PrintWriter("experiment_selectivity_sweep.csv")) {

            writer.println("selectivity,build_size,probe_size,"
                    + "runtime_no_filter_avg,runtime_bloom_avg,speedup_avg,"
                    + "hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            for (double sel : selectivities) {

                System.out.println("\n=== Experiment 6: Selectivity = " + sel + " ===");

                // Build prefix for dataset
                String prefix = "selSweep_" + sel;

                // Generate data for this selectivity if missing
                generateTestDataIfMissing(buildSize, probeSize, sel, prefix);

                // Load data
                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Averaging strategy:
                int runs = 7;  // safe for build size = 1M

                // Run NoFilter
                System.out.println("Running NoFilter (" + runs + " runs)...");
                JoinMetrics noFilterAvg = averageNoFilter(dates, sales, runs);

                // Run Bloom
                System.out.println("Running Bloom (" + runs + " runs)...");
                JoinMetrics bloomAvg = averageBloom(dates, sales, filterSize, k, runs);

                double speedup = noFilterAvg.runtime / bloomAvg.runtime;

                writer.println(String.format(Locale.US,
                        "%f,%d,%d,%.4f,%.4f,%.3f,%d,%d,%d,%d",
                        sel,
                        buildSize,
                        probeSize,
                        noFilterAvg.runtime,
                        bloomAvg.runtime,
                        speedup,
                        bloomAvg.hashLookups,
                        bloomAvg.bloomPasses,
                        bloomAvg.bloomRejects,
                        bloomAvg.actualMatches
                ));
            }

            System.out.println("\nExperiment 6 DONE → experiment_selectivity_sweep.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // ============================================================
// EXPERIMENT 7 — REDUCED BLOOM FILTER SIZE EXPERIMENT
// ============================================================
    private static void runExperimentReducedBloomSize() {

        int buildSize = 1_000_000;
        int probeSize = 10_000_000;
        double selectivity = 0.001;  // small enough that Bloom can win

        int[] bloomSizes = {
                2_000_000,    // 0.25 MB
                4_000_000,    // 0.5 MB
                8_000_000,    // 1 MB (likely the sweet spot)
                16_000_000    // 2 MB (your original)
        };

        int k = 2; // fast and decent

        try (PrintWriter writer = new PrintWriter("experiment_reduced_bloom_size.csv")) {

            writer.println("filter_size_bits,build_size,probe_size,"
                    + "runtime_no_filter_avg,runtime_bloom_avg,speedup_avg,"
                    + "hash_lookups_avg,bloom_passes_avg,bloom_rejects_avg,actual_matches");

            // prefix for generating data
            String prefix = "reducedBloom";
            generateTestDataIfMissing(buildSize, probeSize, selectivity, prefix);

            List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
            List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

            int runs = 5; // stable but not too slow

            // Compute no-filter baseline once
            System.out.println("Running NoFilter baseline (" + runs + " runs)...");
            JoinMetrics noFilterAvg = averageNoFilter(dates, sales, runs);

            for (int m : bloomSizes) {

                System.out.println("\n=== Experiment 8: Bloom Filter Size = " + m + " bits ===");

                // average bloom
                JoinMetrics bloomAvg = averageBloom(dates, sales, m, k, runs);

                double speedup = noFilterAvg.runtime / bloomAvg.runtime;

                writer.println(String.format(Locale.US,
                        "%d,%d,%d,%.4f,%.4f,%.3f,%d,%d,%d,%d",
                        m,
                        buildSize,
                        probeSize,
                        noFilterAvg.runtime,
                        bloomAvg.runtime,
                        speedup,
                        bloomAvg.hashLookups,
                        bloomAvg.bloomPasses,
                        bloomAvg.bloomRejects,
                        bloomAvg.actualMatches
                ));
            }

            System.out.println("\nExperiment 7 DONE → experiment_reduced_bloom_size.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
// EXPERIMENT 9 — BIG BUILD SIZE, 128M FILTER, NO AVERAGING
// ============================================================
    private static void runExperiment9_BigBuildNoAvg() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000,
                5_000_000,
                10_000_000
        };

        int probeSize = 10_000_000;
        double selectivity = 0.05;   // same as original experiments

        int filterSize = 128_000_000;  // 128M bits
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_9_big_build_noavg.csv")) {

            writer.println("build_size,probe_size,"
                    + "runtime_no_filter,runtime_bloom,speedup,"
                    + "hash_lookups,bloom_passes,bloom_rejects,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "exp9_" + buildSize;

                System.out.println("\n=== Experiment 9: Build Size = " + buildSize + " ===");

                // Generate if missing
                generateTestDataIfMissing(buildSize, probeSize, selectivity, prefix);

                // Load data
                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Try NoFilter once
                String runtimeNoFilterStr;
                JoinMetrics noFilter = null;

                try {
                    noFilter = runNoFilterJoin(dates, sales);
                    runtimeNoFilterStr = String.format(Locale.US, "%.4f", noFilter.runtime);
                } catch (OutOfMemoryError oom) {
                    System.out.println("❌ NoFilter OOM at buildSize=" + buildSize);
                    runtimeNoFilterStr = "OOM";
                }

                // Try Bloom once
                String runtimeBloomStr;
                JoinMetrics bloom = null;

                try {
                    bloom = runBloomJoin(dates, sales, filterSize, k);
                    runtimeBloomStr = String.format(Locale.US, "%.4f", bloom.runtime);
                } catch (OutOfMemoryError oom) {
                    System.out.println("❌ Bloom OOM at buildSize=" + buildSize);
                    runtimeBloomStr = "OOM";
                }

                String speedupStr = "NA";
                if (!runtimeNoFilterStr.equals("OOM") && !runtimeBloomStr.equals("OOM")) {
                    speedupStr = String.format(Locale.US,
                            "%.3f", noFilter.runtime / bloom.runtime);
                }

                writer.println(buildSize + "," + probeSize + ","
                        + runtimeNoFilterStr + ","
                        + runtimeBloomStr + ","
                        + speedupStr + ","
                        + (bloom != null ? bloom.hashLookups : -1) + ","
                        + (bloom != null ? bloom.bloomPasses : -1) + ","
                        + (bloom != null ? bloom.bloomRejects : -1) + ","
                        + (bloom != null ? bloom.actualMatches : -1)
                );
            }

            System.out.println("\nExperiment 9 DONE → experiment_9_big_build_noavg.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    // ============================================================
// EXPERIMENT 10 — BIG BUILD SIZES, MULTIPLE FILTER SIZES, NO AVERAGING
// ============================================================
    private static void runExperiment10_BigBuildNoAvg() {

        int[] buildSizes = {
                10_000,
                33_000,
                100_000,
                330_000,
                1_000_000,
                3_000_000
                // 5M and 10M removed — Java OOM with Strings
        };

        int[] bloomSizes = {
                4_000_000,       // 0.5 MB
                8_000_000,       // 1 MB
                16_000_000,      // 2 MB
                128_000_000      // 16 MB (your sweet spot)
        };

        int probeSize = 10_000_000;
        double selectivity = 0.05;
        int k = 2;

        try (PrintWriter writer = new PrintWriter("experiment_10_big_build_noavg.csv")) {

            writer.println("build_size,probe_size,filter_size_bits,"
                    + "runtime_no_filter,runtime_bloom,speedup,"
                    + "hash_lookups,bloom_passes,bloom_rejects,actual_matches");

            for (int buildSize : buildSizes) {

                String prefix = "exp10_" + buildSize;
                System.out.println("\n=== Experiment 10: Build Size = " + buildSize + " ===");

                // Generate data if needed
                generateTestDataIfMissing(buildSize, probeSize, selectivity, prefix);

                // Load tables once
                List<String[]> dates = CSVReader.readCSV("data/" + prefix + "_dates.csv");
                List<String[]> sales = CSVReader.readCSV("data/" + prefix + "_sales.csv");

                // Try NoFilter once (no averaging)
                String noFilterRuntimeStr;
                JoinMetrics noFilter = null;

                try {
                    noFilter = runNoFilterJoin(dates, sales);
                    noFilterRuntimeStr = String.format(Locale.US, "%.4f", noFilter.runtime);
                } catch (OutOfMemoryError oom) {
                    System.out.println("❌ NoFilter OOM at buildSize=" + buildSize);
                    noFilterRuntimeStr = "OOM";
                }

                // Now test each Bloom size
                for (int m : bloomSizes) {

                    System.out.println("  → Testing Bloom(m=" + m + ", k=2)");

                    JoinMetrics bloom = null;
                    String bloomRuntimeStr;

                    try {
                        bloom = runBloomJoin(dates, sales, m, k);
                        bloomRuntimeStr = String.format(Locale.US, "%.4f", bloom.runtime);
                    } catch (OutOfMemoryError oom) {
                        System.out.println("❌ Bloom OOM for m=" + m + " bits at buildSize=" + buildSize);
                        bloomRuntimeStr = "OOM";
                    }

                    // Compute speedup if both succeeded
                    String speedupStr = "NA";
                    if (!noFilterRuntimeStr.equals("OOM") && !bloomRuntimeStr.equals("OOM")) {
                        speedupStr = String.format(Locale.US,
                                "%.3f", noFilter.runtime / bloom.runtime);
                    }

                    writer.println(buildSize + "," + probeSize + "," + m + ","
                            + noFilterRuntimeStr + ","
                            + bloomRuntimeStr + ","
                            + speedupStr + ","
                            + (bloom != null ? bloom.hashLookups : -1) + ","
                            + (bloom != null ? bloom.bloomPasses : -1) + ","
                            + (bloom != null ? bloom.bloomRejects : -1) + ","
                            + (bloom != null ? bloom.actualMatches : -1)
                    );
                }
            }

            System.out.println("\nExperiment 10 DONE → experiment_10_big_build_noavg.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


 */
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
