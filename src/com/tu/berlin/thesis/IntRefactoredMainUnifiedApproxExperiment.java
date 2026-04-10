package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.*;
import java.util.function.Supplier;

public class IntRefactoredMainUnifiedApproxExperiment {

    private static final int WARMUP_RUNS = 1;
    private static final int MEASURED_RUNS = 3;

    public static void main(String[] args) {
        System.out.println("=== EXP INT UNIFIED: NoFilter vs Bloom vs ExactRanges vs BulkApprox vs StreamedApprox ===");
        runExperiment("2");
    }

    private static void runExperiment(String suffix) {

        int[] buildSizes = {1_000_000};

        int[] bloomSizes = {
                160_000_000,
                1_600_000_000
        };

        int[] clusterCounts    = {16, 32, 64, 128, 256};
        int[] targetRangeCounts = {4, 8, 10, 12, 16, 20, 25, 32, 40, 50, 54, 64, 80, 100, 110, 160, 200, 240};
        int[] gapLens           = {100, 1000, 10_000, 100_000};

        int    probeSize   = 10_000_000;
        double selectivity = 0.05;
        int    k           = 2;

        String outName = "experiment_unified_approx_filters_" + suffix + ".csv";

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
                            + "hash_lookups_approx_bulk,approx_bulk_passes,approx_bulk_rejects,exact_range_count_bulk,approx_range_count_bulk,approx_range_bytes_bulk,"
                            + "hash_lookups_approx_streamed,approx_streamed_passes,approx_streamed_rejects,approx_range_count_streamed,approx_range_bytes_streamed,"
                            + "approx_lookup_ratio_bulk,approx_lookup_ratio_streamed"
            );

            for (int buildSize : buildSizes) {

                // ============================================================
                // 1. Generate base data ONCE per buildSize
                //    - Fixed build IDs: 1..buildSize (no clustering)
                //    - Fixed probe file: same matches + non-matches every run
                // ============================================================
                String basePrefix = "base_B" + buildSize
                        + "_P" + probeSize
                        + "_S" + (int) (selectivity * 100);

                generateBaseDataIfMissing(buildSize, probeSize, selectivity, basePrefix);

                // Read probe once — never changes regardless of clusters/gaps
                List<int[]> sales     = IntCSVReader.readCSV("data/" + basePrefix + "_sales_int.csv");
                List<int[]> baseDates = IntCSVReader.readCSV("data/" + basePrefix + "_dates_int.csv");

                // ============================================================
                // 2. Baseline filters: computed ONCE per buildSize.
                //    They only depend on the set of IDs, not their layout.
                // ============================================================
                TimedRun  noF   = measureTimedRun (() -> runNoFilterBuildAndProbe(baseDates, sales));
                RangeRunTimed exact = measureRangeRun(() -> runExactRangesBuildAndProbe(baseDates, sales, buildSize));

                // Bloom is also per-buildSize (one result per m value)
                Map<Integer, BloomRunTimed> bloomResults = new HashMap<>();
                for (int m : bloomSizes) {
                    BloomRunTimed bloom = measureBloomRun(() -> runBloomBuildAndProbe(baseDates, sales, m, k));
                    bloomResults.put(m, bloom);
                }

                // ============================================================
                // 3. Clustered layouts: only the spatial arrangement changes.
                //    Same IDs, same probe — just remapped into (cluster, gap) slots.
                //    Approx filters are the only ones that care about layout.
                // ============================================================
                for (int clusters : clusterCounts) {
                    for (int gapLen : gapLens) {

                        int clusterLen = buildSize / clusters;

                        String layoutPrefix = "layout"
                                + "_B" + buildSize
                                + "_C" + clusters
                                + "_G" + gapLen;

                        generateClusteredLayoutIfMissing(
                                basePrefix, layoutPrefix,
                                buildSize, clusters, clusterLen, gapLen
                        );

                        List<int[]> layoutDates = IntCSVReader.readCSV("data/" + layoutPrefix + "_dates_int.csv");

                        // Approx filters per targetRangeCount
                        for (int targetRangeCount : targetRangeCounts) {

                            if (targetRangeCount >= clusters) continue;

                            ApproxRunTimed bulk = measureApproxRun(
                                    () -> runApproxRangesBuildAndProbe(layoutDates, sales, buildSize, targetRangeCount)
                            );

                            ApproxStreamedRunTimed streamed = measureApproxStreamedRun(
                                    () -> runApproxRangesStreamedBuildAndProbe(layoutDates, sales, buildSize, targetRangeCount)
                            );

                            for (int m : bloomSizes) {
                                BloomRunTimed bloom = bloomResults.get(m);

                                double speedBloomProbe    = noF.probeMs / bloom.probeMs;
                                double speedExactProbe    = noF.probeMs / exact.probeMs;
                                double speedBulkProbe     = noF.probeMs / bulk.probeMs;
                                double speedStreamedProbe = noF.probeMs / streamed.probeMs;

                                double approxLookupRatioBulk =
                                        exact.hashLookups == 0 ? 0.0
                                                : ((double) bulk.hashLookups / exact.hashLookups);

                                double approxLookupRatioStreamed =
                                        exact.hashLookups == 0 ? 0.0
                                                : ((double) streamed.hashLookups / exact.hashLookups);

                                w.println(
                                        buildSize + "," + probeSize + "," + m + ","
                                                + clusters + "," + clusterLen + "," + gapLen + "," + targetRangeCount + ","
                                                + fmt(noF.buildMs)      + "," + fmt(noF.probeMs)      + ","
                                                + fmt(bloom.buildMs)    + "," + fmt(bloom.probeMs)    + ","
                                                + fmt(exact.buildMs)    + "," + fmt(exact.probeMs)    + ","
                                                + fmt(bulk.buildMs)     + "," + fmt(bulk.probeMs)     + ","
                                                + fmt(streamed.buildMs) + "," + fmt(streamed.probeMs) + ","
                                                + fmt3(speedBloomProbe)    + "," + fmt3(speedExactProbe) + ","
                                                + fmt3(speedBulkProbe)     + "," + fmt3(speedStreamedProbe) + ","
                                                + bloom.hashLookups + "," + bloom.passes + "," + bloom.rejects + ","
                                                + exact.hashLookups + "," + exact.passes + "," + exact.rejects + "," + exact.rangeCount + "," + exact.rangeBytes + ","
                                                + bulk.hashLookups  + "," + bulk.passes  + "," + bulk.rejects  + ","
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
    // DATA GENERATION
    // ============================================================

    /**
     * Generates a base build file (IDs 1..buildSize, no clustering) and a
     * probe file (fixed matches + fixed non-matches) ONCE per experiment config.
     *
     * Nothing in here changes when clusters/gapLen change — that's the point.
     * Bloom, NoFilter, and ExactRanges all run on this data.
     */
    private static void generateBaseDataIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            String basePrefix
    ) {
        String datesPath = "data/" + basePrefix + "_dates_int.csv";
        String salesPath = "data/" + basePrefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("Base data exists -> " + basePrefix);
            return;
        }

        System.out.println("Generating base data -> " + basePrefix);

        int[] buildIds = new int[buildSize];

        try (PrintWriter w = new PrintWriter(datesPath)) {
            w.println("id,year,desc_code");
            for (int i = 0; i < buildSize; i++) {
                int id = i + 1;           // IDs: 1, 2, 3, ..., buildSize
                buildIds[i] = id;
                w.println(id + "," + (1980 + id % 40) + "," + (id % 1000));
            }
        } catch (Exception e) { e.printStackTrace(); }

        int matchCount    = (int) (probeSize * selectivity);
        int nonMatchCount = probeSize - matchCount;

        try (PrintWriter w = new PrintWriter(salesPath)) {
            w.println("sale_id,date_id,product_code,price,customer_code");

            // Matches: cycle through valid build IDs
            for (int i = 0; i < matchCount; i++) {
                int id = buildIds[i % buildSize];
                w.println((i + 1) + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
            }

            // Non-matches: IDs well outside [1..buildSize]
            int nonMatchBase = buildSize + 1_000_000;
            for (int t = 0; t < nonMatchCount; t++) {
                int rowId      = matchCount + t + 1;
                int nonMatchId = nonMatchBase + t + 1;
                w.println(rowId + "," + nonMatchId + "," + (rowId % 5000) + ",10," + (rowId % 20000));
            }
        } catch (Exception e) { e.printStackTrace(); }

        System.out.println("  Base data done: build=" + buildSize + " probe=" + probeSize);
    }

    /**
     * Remaps the base build IDs into a clustered spatial layout for a specific
     * (clusters, gapLen) pair.
     *
     * The probe file is NOT touched — it stays as-is from generateBaseDataIfMissing.
     * The same probe values that matched IDs 1..buildSize will now match the
     * remapped clustered IDs, because we remap the BUILD side, not the probe side.
     *
     * Wait — actually the probe references the OLD IDs, so we must keep the build
     * IDs the same and instead write the dates file with the SAME IDs but in a
     * different ORDER that reflects clustering (i.e. the hash table will see
     * the same keys, but when the range filter scans them they appear clustered).
     *
     * More precisely: we write the build rows in cluster-ordered sequence so the
     * range filter discovers (clusters) contiguous ranges instead of one giant range.
     */
    private static void generateClusteredLayoutIfMissing(
            String basePrefix,
            String layoutPrefix,
            int buildSize,
            int clusters,
            int clusterLen,
            int gapLen
    ) {
        String layoutDatesPath = "data/" + layoutPrefix + "_dates_int.csv";

        if (fileExists(layoutDatesPath)) {
            System.out.println("Layout exists -> " + layoutPrefix);
            return;
        }

        System.out.println("Generating clustered layout -> " + layoutPrefix
                + " (clusters=" + clusters + ", clusterLen=" + clusterLen + ", gapLen=" + gapLen + ")");

        // Each cluster occupies IDs in [base + c*stride .. base + c*stride + clusterLen - 1]
        // The gaps [base + c*stride + clusterLen .. base + (c+1)*stride - 1] contain no build IDs.
        int stride     = clusterLen + gapLen;
        int baseOffset = 1_000_000_000;   // far from the non-match range (buildSize + 1_000_000)

        try (PrintWriter w = new PrintWriter(layoutDatesPath)) {
            w.println("id,year,desc_code");

            int written = 0;
            outer:
            for (int c = 0; c < clusters; c++) {
                int clusterStart = baseOffset + c * stride;
                for (int offset = 0; offset < clusterLen; offset++) {
                    if (written >= buildSize) break outer;
                    int newId    = clusterStart + offset;
                    int year     = 1980 + (newId % 40);
                    int descCode = newId % 1000;
                    w.println(newId + "," + year + "," + descCode);
                    written++;
                }
            }
        } catch (Exception e) { e.printStackTrace(); }

        System.out.println("  Layout done.");
    }

    // ============================================================
    // IMPORTANT: the probe file also needs updating for clustered layouts
    // because the probe references build IDs, which changed.
    // We generate a matching probe per layout.
    // ============================================================

    /**
     * Generates a probe file whose matches reference the CLUSTERED IDs from
     * a given layout. Non-matches stay in the far-away range.
     *
     * This is necessary because the layout remaps build IDs to a new numeric
     * space (1_000_000_000 + offset), so the original probe (which used IDs
     * 1..buildSize) would never match.
     */
    private static void generateLayoutProbeIfMissing(
            String layoutPrefix,
            int buildSize,
            int probeSize,
            double selectivity,
            int clusters,
            int clusterLen,
            int gapLen
    ) {
        String salesPath = "data/" + layoutPrefix + "_sales_int.csv";

        if (fileExists(salesPath)) return;

        int stride     = clusterLen + gapLen;
        int baseOffset = 1_000_000_000;

        // Collect the valid IDs in the same order as the layout file
        int[] validIds = new int[buildSize];
        int   idx      = 0;
        outer:
        for (int c = 0; c < clusters; c++) {
            int clusterStart = baseOffset + c * stride;
            for (int offset = 0; offset < clusterLen; offset++) {
                if (idx >= buildSize) break outer;
                validIds[idx++] = clusterStart + offset;
            }
        }

        int matchCount    = (int) (probeSize * selectivity);
        int nonMatchCount = probeSize - matchCount;

        try (PrintWriter w = new PrintWriter(salesPath)) {
            w.println("sale_id,date_id,product_code,price,customer_code");

            for (int i = 0; i < matchCount; i++) {
                int id = validIds[i % idx];
                w.println((i + 1) + "," + id + "," + (i % 5000) + ",10," + (i % 20000));
            }

            // Non-matches: use IDs in the gap regions (realistic) + far range
            int gapNonMatchCount = nonMatchCount / 2;
            int farNonMatchCount = nonMatchCount - gapNonMatchCount;
            int rowId = matchCount + 1;

            if (clusters > 1 && gapLen > 0) {
                for (int t = 0; t < gapNonMatchCount; t++) {
                    int gapIndex  = t % (clusters - 1);
                    int gapStart  = baseOffset + gapIndex * stride + clusterLen;
                    int nonMatchId = gapStart + (t % gapLen);
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
        } catch (Exception e) { e.printStackTrace(); }
    }

    // ============================================================
    // MEASUREMENT WRAPPERS: warmup + median
    // ============================================================

    private static TimedRun measureTimedRun(Supplier<TimedRun> supplier) {
        for (int i = 0; i < WARMUP_RUNS; i++) supplier.get();
        List<TimedRun> runs = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) runs.add(supplier.get());
        runs.sort(Comparator.comparingDouble(r -> r.probeMs));
        return runs.get(runs.size() / 2);
    }

    private static BloomRunTimed measureBloomRun(Supplier<BloomRunTimed> supplier) {
        for (int i = 0; i < WARMUP_RUNS; i++) supplier.get();
        List<BloomRunTimed> runs = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) runs.add(supplier.get());
        runs.sort(Comparator.comparingDouble(r -> r.probeMs));
        return runs.get(runs.size() / 2);
    }

    private static RangeRunTimed measureRangeRun(Supplier<RangeRunTimed> supplier) {
        for (int i = 0; i < WARMUP_RUNS; i++) supplier.get();
        List<RangeRunTimed> runs = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) runs.add(supplier.get());
        runs.sort(Comparator.comparingDouble(r -> r.probeMs));
        return runs.get(runs.size() / 2);
    }

    private static ApproxRunTimed measureApproxRun(Supplier<ApproxRunTimed> supplier) {
        for (int i = 0; i < WARMUP_RUNS; i++) supplier.get();
        List<ApproxRunTimed> runs = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) runs.add(supplier.get());
        runs.sort(Comparator.comparingDouble(r -> r.probeMs));
        return runs.get(runs.size() / 2);
    }

    private static ApproxStreamedRunTimed measureApproxStreamedRun(Supplier<ApproxStreamedRunTimed> supplier) {
        for (int i = 0; i < WARMUP_RUNS; i++) supplier.get();
        List<ApproxStreamedRunTimed> runs = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) runs.add(supplier.get());
        runs.sort(Comparator.comparingDouble(r -> r.probeMs));
        return runs.get(runs.size() / 2);
    }

    // ============================================================
    // RUNNERS
    // ============================================================

    private static TimedRun runNoFilterBuildAndProbe(List<int[]> dates, List<int[]> sales) {
        IntHashJoinOperator join = new IntHashJoinOperator(
                new IntScanOperator(dates), new IntScanOperator(sales), 0, 1);
        long b0 = System.nanoTime(); join.open(); long b1 = System.nanoTime();
        long p0 = System.nanoTime(); while (join.next() != null) { } long p1 = System.nanoTime();
        join.close();
        return new TimedRun((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0);
    }

    private static BloomRunTimed runBloomBuildAndProbe(List<int[]> dates, List<int[]> sales, int mBits, int k) {
        IntHashJoinWithBloomFilter join = new IntHashJoinWithBloomFilter(
                new IntScanOperator(dates), new IntScanOperator(sales), 0, 1, mBits, k);
        long b0 = System.nanoTime(); join.open(); long b1 = System.nanoTime();
        long p0 = System.nanoTime(); while (join.next() != null) { } long p1 = System.nanoTime();
        join.close();
        return new BloomRunTimed((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0,
                join.getHashLookups(), join.getBloomPasses(), join.getBloomRejects());
    }

    private static RangeRunTimed runExactRangesBuildAndProbe(List<int[]> dates, List<int[]> sales, int expectedBuildKeys) {
        IntHashJoinWithExactRanges join = new IntHashJoinWithExactRanges(
                new IntScanOperator(dates), new IntScanOperator(sales), 0, 1, expectedBuildKeys);
        long b0 = System.nanoTime(); join.open(); long b1 = System.nanoTime();
        long p0 = System.nanoTime(); while (join.next() != null) { } long p1 = System.nanoTime();
        join.close();
        return new RangeRunTimed((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0,
                join.getHashLookups(), join.getRangePasses(), join.getRangeRejects(),
                join.getRangeCount(), join.getRangeBytes());
    }

    private static ApproxRunTimed runApproxRangesBuildAndProbe(
            List<int[]> dates, List<int[]> sales, int expectedBuildKeys, int targetRangeCount) {
        IntHashJoinWithApproximateRanges join = new IntHashJoinWithApproximateRanges(
                new IntScanOperator(dates), new IntScanOperator(sales), 0, 1,
                expectedBuildKeys, targetRangeCount);
        long b0 = System.nanoTime(); join.open(); long b1 = System.nanoTime();
        long p0 = System.nanoTime(); while (join.next() != null) { } long p1 = System.nanoTime();
        join.close();
        return new ApproxRunTimed((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0,
                join.getHashLookups(), join.getRangePasses(), join.getRangeRejects(),
                join.getExactRangeCount(), join.getApproximateRangeCount(), join.getRangeBytes());
    }

    private static ApproxStreamedRunTimed runApproxRangesStreamedBuildAndProbe(
            List<int[]> dates, List<int[]> sales, int expectedBuildKeys, int targetRangeCount) {
        IntHashJoinWithApproximateRangesStreamed join = new IntHashJoinWithApproximateRangesStreamed(
                new IntScanOperator(dates), new IntScanOperator(sales), 0, 1,
                expectedBuildKeys, targetRangeCount);
        long b0 = System.nanoTime(); join.open(); long b1 = System.nanoTime();
        long p0 = System.nanoTime(); while (join.next() != null) { } long p1 = System.nanoTime();
        join.close();
        return new ApproxStreamedRunTimed((b1 - b0) / 1_000_000.0, (p1 - p0) / 1_000_000.0,
                join.getHashLookups(), join.getRangePasses(), join.getRangeRejects(),
                join.getApproximateRangeCount(), join.getRangeBytes());
    }

    // ============================================================
    // HELPERS
    // ============================================================

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static String fmt(double x)  { return String.format(Locale.US, "%.4f", x); }
    private static String fmt3(double x) { return String.format(Locale.US, "%.3f", x); }

    // ============================================================
    // RESULT RECORDS
    // ============================================================

    private static final class TimedRun {
        final double buildMs, probeMs;
        TimedRun(double b, double p) { buildMs = b; probeMs = p; }
    }

    private static final class BloomRunTimed {
        final double buildMs, probeMs;
        final int hashLookups, passes, rejects;
        BloomRunTimed(double b, double p, int h, int pa, int r) {
            buildMs = b; probeMs = p; hashLookups = h; passes = pa; rejects = r;
        }
    }

    private static final class RangeRunTimed {
        final double buildMs, probeMs;
        final int hashLookups, passes, rejects, rangeCount;
        final long rangeBytes;
        RangeRunTimed(double b, double p, int h, int pa, int r, int rc, long rb) {
            buildMs = b; probeMs = p; hashLookups = h; passes = pa; rejects = r;
            rangeCount = rc; rangeBytes = rb;
        }
    }

    private static final class ApproxRunTimed {
        final double buildMs, probeMs;
        final int hashLookups, passes, rejects, exactRangeCount, approxRangeCount;
        final long rangeBytes;
        ApproxRunTimed(double b, double p, int h, int pa, int r, int erc, int arc, long rb) {
            buildMs = b; probeMs = p; hashLookups = h; passes = pa; rejects = r;
            exactRangeCount = erc; approxRangeCount = arc; rangeBytes = rb;
        }
    }

    private static final class ApproxStreamedRunTimed {
        final double buildMs, probeMs;
        final int hashLookups, passes, rejects, approxRangeCount;
        final long rangeBytes;
        ApproxStreamedRunTimed(double b, double p, int h, int pa, int r, int arc, long rb) {
            buildMs = b; probeMs = p; hashLookups = h; passes = pa; rejects = r;
            approxRangeCount = arc; rangeBytes = rb;
        }
    }
}