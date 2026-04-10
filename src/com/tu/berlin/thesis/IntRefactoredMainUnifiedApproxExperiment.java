package com.tu.berlin.thesis;

import com.tu.berlin.thesis.data.IntCSVReader;
import com.tu.berlin.thesis.operators.*;

import java.io.PrintWriter;
import java.util.*;
import java.util.function.Supplier;

public class IntRefactoredMainUnifiedApproxExperiment {

    private static final int WARMUP_RUNS = 1;
    private static final int MEASURED_RUNS = 3;

    private static final int PROBE_CLASS_MATCH = 1;
    private static final int PROBE_CLASS_NEAR_MISS = 2;
    private static final int PROBE_CLASS_FAR_MISS = 3;

    public static void main(String[] args) {
        System.out.println("=== EXP INT UNIFIED: NoFilter vs Bloom vs ExactRanges vs BulkApprox vs StreamedApprox ===");
        runExperiment("9");
    }

    private static void runExperiment(String suffix) {

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
        double nearMissFractionOfNonMatches = 0.30;
        int k = 2;

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

                String logicalPrefix = "logical_B" + buildSize
                        + "_P" + probeSize
                        + "_S" + (int) (selectivity * 100)
                        + "_NM" + (int) (nearMissFractionOfNonMatches * 100);

                generateLogicalBaseDataIfMissing(
                        buildSize,
                        probeSize,
                        selectivity,
                        nearMissFractionOfNonMatches,
                        logicalPrefix
                );

                for (int clusters : clusterCounts) {
                    for (int gapLen : gapLens) {

                        String layoutPrefix = "layout"
                                + "_B" + buildSize
                                + "_C" + clusters
                                + "_G" + gapLen
                                + "_P" + probeSize
                                + "_S" + (int) (selectivity * 100)
                                + "_NM" + (int) (nearMissFractionOfNonMatches * 100);

                        LayoutInfo layoutInfo = generatePhysicalLayoutDataIfMissing(
                                logicalPrefix,
                                layoutPrefix,
                                buildSize,
                                clusters,
                                gapLen
                        );

                        List<int[]> dates = IntCSVReader.readCSV("data/" + layoutPrefix + "_dates_int.csv");
                        List<int[]> sales = IntCSVReader.readCSV("data/" + layoutPrefix + "_sales_int.csv");

                        TimedRun noF = measureTimedRun(() -> runNoFilterBuildAndProbe(dates, sales));
                        RangeRunTimed exact = measureRangeRun(() -> runExactRangesBuildAndProbe(dates, sales, buildSize));

                        Map<Integer, BloomRunTimed> bloomResults = new HashMap<>();
                        for (int m : bloomSizes) {
                            BloomRunTimed bloom = measureBloomRun(() -> runBloomBuildAndProbe(dates, sales, m, k));
                            bloomResults.put(m, bloom);
                        }

                        for (int targetRangeCount : targetRangeCounts) {

                            if (targetRangeCount >= clusters) continue;

                            ApproxRunTimed bulk = measureApproxRun(
                                    () -> runApproxRangesBuildAndProbe(dates, sales, buildSize, targetRangeCount)
                            );

                            ApproxStreamedRunTimed streamed = measureApproxStreamedRun(
                                    () -> runApproxRangesStreamedBuildAndProbe(dates, sales, buildSize, targetRangeCount)
                            );

                            for (int m : bloomSizes) {
                                BloomRunTimed bloom = bloomResults.get(m);

                                double speedBloomProbe = noF.probeMs / bloom.probeMs;
                                double speedExactProbe = noF.probeMs / exact.probeMs;
                                double speedBulkProbe = noF.probeMs / bulk.probeMs;
                                double speedStreamedProbe = noF.probeMs / streamed.probeMs;

                                double approxLookupRatioBulk =
                                        exact.hashLookups == 0 ? 0.0 : ((double) bulk.hashLookups / exact.hashLookups);

                                double approxLookupRatioStreamed =
                                        exact.hashLookups == 0 ? 0.0 : ((double) streamed.hashLookups / exact.hashLookups);

                                w.println(
                                        buildSize + "," + probeSize + "," + m + ","
                                                + clusters + "," + layoutInfo.avgClusterLen + "," + gapLen + "," + targetRangeCount + ","
                                                + fmt(noF.buildMs) + "," + fmt(noF.probeMs) + ","
                                                + fmt(bloom.buildMs) + "," + fmt(bloom.probeMs) + ","
                                                + fmt(exact.buildMs) + "," + fmt(exact.probeMs) + ","
                                                + fmt(bulk.buildMs) + "," + fmt(bulk.probeMs) + ","
                                                + fmt(streamed.buildMs) + "," + fmt(streamed.probeMs) + ","
                                                + fmt3(speedBloomProbe) + "," + fmt3(speedExactProbe) + ","
                                                + fmt3(speedBulkProbe) + "," + fmt3(speedStreamedProbe) + ","
                                                + bloom.hashLookups + "," + bloom.passes + "," + bloom.rejects + ","
                                                + exact.hashLookups + "," + exact.passes + "," + exact.rejects + "," + exact.rangeCount + "," + exact.rangeBytes + ","
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
    // LOGICAL BASE DATA
    // ============================================================

    private static void generateLogicalBaseDataIfMissing(
            int buildSize,
            int probeSize,
            double selectivity,
            double nearMissFractionOfNonMatches,
            String logicalPrefix
    ) {
        String logicalBuildPath = "data/" + logicalPrefix + "_build_logical.csv";
        String logicalProbePath = "data/" + logicalPrefix + "_probe_logical.csv";

        if (fileExists(logicalBuildPath) && fileExists(logicalProbePath)) {
            System.out.println("Logical base data exists -> " + logicalPrefix);
            return;
        }

        System.out.println("Generating logical base data -> " + logicalPrefix);

        try (PrintWriter w = new PrintWriter(logicalBuildPath)) {
            w.println("logical_id,year,desc_code");
            for (int i = 0; i < buildSize; i++) {
                int logicalId = i;
                int year = 1980 + (logicalId % 40);
                int descCode = logicalId % 1000;
                w.println(logicalId + "," + year + "," + descCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        int matchCount = (int) (probeSize * selectivity);
        int nonMatchCount = probeSize - matchCount;

        int nearMissCount = (int) Math.round(nonMatchCount * nearMissFractionOfNonMatches);
        int farMissCount = nonMatchCount - nearMissCount;

        Random rnd = new Random(42);

        int[] permutedBuildIds = new int[buildSize];
        for (int i = 0; i < buildSize; i++) permutedBuildIds[i] = i;
        shuffleArray(permutedBuildIds, rnd);

        try (PrintWriter w = new PrintWriter(logicalProbePath)) {
            w.println("sale_id,logical_date_id,probe_class,product_code,price,customer_code");

            int saleId = 1;

            for (int i = 0; i < matchCount; i++) {
                int logicalDateId = permutedBuildIds[i % buildSize];
                w.println(saleId + "," + logicalDateId + "," + PROBE_CLASS_MATCH + ","
                        + (saleId % 5000) + ",10," + (saleId % 20000));
                saleId++;
            }

            for (int i = 0; i < nearMissCount; i++) {
                int logicalNearMissId = buildSize + 1 + i;
                w.println(saleId + "," + logicalNearMissId + "," + PROBE_CLASS_NEAR_MISS + ","
                        + (saleId % 5000) + ",10," + (saleId % 20000));
                saleId++;
            }

            for (int i = 0; i < farMissCount; i++) {
                int logicalFarMissId = buildSize + 1 + nearMissCount + i;
                w.println(saleId + "," + logicalFarMissId + "," + PROBE_CLASS_FAR_MISS + ","
                        + (saleId % 5000) + ",10," + (saleId % 20000));
                saleId++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("  Logical base done. matches=" + matchCount
                + " nearMisses=" + nearMissCount
                + " farMisses=" + farMissCount);
    }

    // ============================================================
    // PHYSICAL LAYOUT MATERIALIZATION
    // ============================================================

    private static LayoutInfo generatePhysicalLayoutDataIfMissing(
            String logicalPrefix,
            String layoutPrefix,
            int buildSize,
            int clusters,
            int gapLen
    ) {
        String logicalBuildPath = "data/" + logicalPrefix + "_build_logical.csv";
        String logicalProbePath = "data/" + logicalPrefix + "_probe_logical.csv";

        String datesPath = "data/" + layoutPrefix + "_dates_int.csv";
        String salesPath = "data/" + layoutPrefix + "_sales_int.csv";

        if (fileExists(datesPath) && fileExists(salesPath)) {
            System.out.println("Physical layout exists -> " + layoutPrefix);
            return computeLayoutInfo(buildSize, clusters);
        }

        System.out.println("Generating physical layout -> " + layoutPrefix
                + " (clusters=" + clusters + ", gapLen=" + gapLen + ")");

        List<int[]> logicalBuildRows;
        List<int[]> logicalProbeRows;

        try {
            logicalBuildRows = IntCSVReader.readCSV(logicalBuildPath);
            logicalProbeRows = IntCSVReader.readCSV(logicalProbePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read logical base files for prefix: " + logicalPrefix, e);
        }

        int[] clusterSizes = computeClusterSizes(buildSize, clusters);

        int[] logicalToPhysical = new int[buildSize];
        int baseOffset = 1_000_000_000;

        List<Integer> gapIds = new ArrayList<>();

        int logicalId = 0;
        int currentStart = baseOffset;

        for (int c = 0; c < clusters; c++) {
            int clusterSize = clusterSizes[c];

            for (int offset = 0; offset < clusterSize; offset++) {
                logicalToPhysical[logicalId] = currentStart + offset;
                logicalId++;
            }

            currentStart += clusterSize;

            if (c < clusters - 1 && gapLen > 0) {
                for (int g = 0; g < gapLen; g++) {
                    gapIds.add(currentStart + g);
                }
                currentStart += gapLen;
            }
        }

        try (PrintWriter w = new PrintWriter(datesPath)) {
            w.println("id,year,desc_code");
            for (int[] row : logicalBuildRows) {
                int lid = row[0];
                int year = row[1];
                int descCode = row[2];
                int physicalId = logicalToPhysical[lid];
                w.println(physicalId + "," + year + "," + descCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (PrintWriter w = new PrintWriter(salesPath)) {
            w.println("sale_id,date_id,product_code,price,customer_code");

            int physicalFarMissBase = 2_000_000_000;
            int nearMissLogicalBase = buildSize + 1;

            for (int[] row : logicalProbeRows) {
                int saleId = row[0];
                int logicalDateId = row[1];
                int probeClass = row[2];
                int productCode = row[3];
                int price = row[4];
                int customerCode = row[5];

                int physicalDateId;

                if (probeClass == PROBE_CLASS_MATCH) {
                    physicalDateId = logicalToPhysical[logicalDateId];
                } else if (probeClass == PROBE_CLASS_NEAR_MISS) {
                    if (gapIds.isEmpty()) {
                        int missOffset = logicalDateId - nearMissLogicalBase;
                        physicalDateId = physicalFarMissBase + missOffset + 1;
                    } else {
                        int nearMissOffset = logicalDateId - nearMissLogicalBase;
                        physicalDateId = gapIds.get(nearMissOffset % gapIds.size());
                    }
                } else {
                    int farMissOffset = logicalDateId - nearMissLogicalBase;
                    physicalDateId = physicalFarMissBase + farMissOffset + 1;
                }

                w.println(saleId + "," + physicalDateId + "," + productCode + "," + price + "," + customerCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("  Physical layout done.");
        return computeLayoutInfo(buildSize, clusters);
    }

    private static LayoutInfo computeLayoutInfo(int buildSize, int clusters) {
        int[] clusterSizes = computeClusterSizes(buildSize, clusters);
        int avg = buildSize / clusters;
        return new LayoutInfo(avg, clusterSizes);
    }

    private static int[] computeClusterSizes(int buildSize, int clusters) {
        int[] clusterSizes = new int[clusters];
        int baseClusterLen = buildSize / clusters;
        int remainder = buildSize % clusters;

        for (int c = 0; c < clusters; c++) {
            clusterSizes[c] = baseClusterLen + (c < remainder ? 1 : 0);
        }
        return clusterSizes;
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
    // HELPERS
    // ============================================================

    private static boolean fileExists(String path) {
        return new java.io.File(path).exists();
    }

    private static String fmt(double x) {
        return String.format(Locale.US, "%.4f", x);
    }

    private static String fmt3(double x) {
        return String.format(Locale.US, "%.3f", x);
    }

    private static void shuffleArray(int[] a, Random rnd) {
        for (int i = a.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }

    // ============================================================
    // RESULT RECORDS
    // ============================================================

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

    private static final class LayoutInfo {
        final int avgClusterLen;
        final int[] clusterSizes;

        LayoutInfo(int avgClusterLen, int[] clusterSizes) {
            this.avgClusterLen = avgClusterLen;
            this.clusterSizes = clusterSizes;
        }
    }
}