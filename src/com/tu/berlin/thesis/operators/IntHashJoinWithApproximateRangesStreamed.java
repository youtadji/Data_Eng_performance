package com.tu.berlin.thesis.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("DuplicatedCode")
public class IntHashJoinWithApproximateRangesStreamed implements IntOperator {

    private final IntOperator buildInput;
    private final IntOperator probeInput;
    private final int buildKeyIndex;
    private final int probeKeyIndex;
    private final int expectedBuildKeys;
    private final int targetRangeCount;

    private final Map<Integer, List<int[]>> hashTable;
    private IntStreamingApproximateRangeSet streamedRanges;

    private int[] currentProbeTuple;
    private List<int[]> currentBuildMatches;
    private int currentBuildMatchIndex;

    private int hashLookups;
    private int rangePasses;
    private int rangeRejects;

    public IntHashJoinWithApproximateRangesStreamed(
            IntOperator buildInput,
            IntOperator probeInput,
            int buildKeyIndex,
            int probeKeyIndex,
            int expectedBuildKeys,
            int targetRangeCount
    ) {
        this.buildInput = buildInput;
        this.probeInput = probeInput;
        this.buildKeyIndex = buildKeyIndex;
        this.probeKeyIndex = probeKeyIndex;
        this.expectedBuildKeys = expectedBuildKeys;
        this.targetRangeCount = targetRangeCount;

        int initialCapacity = Math.max(16, (int) (expectedBuildKeys / 0.75f) + 1);
        this.hashTable = new HashMap<>(initialCapacity);
    }

    @Override
    public void open() {
        buildInput.open();
        probeInput.open();

        hashTable.clear();
        streamedRanges = new IntStreamingApproximateRangeSet(targetRangeCount);

        currentProbeTuple = null;
        currentBuildMatches = null;
        currentBuildMatchIndex = 0;

        hashLookups = 0;
        rangePasses = 0;
        rangeRejects = 0;

        int[] buildTuple;
        while ((buildTuple = buildInput.next()) != null) {
            int key = buildTuple[buildKeyIndex];

            hashTable
                    .computeIfAbsent(key, k -> new ArrayList<>(1))
                    .add(buildTuple);

            streamedRanges.insert(key);
        }
    }

    @Override
    public int[] next() {
        while (true) {
            // still returning matches for current probe tuple
            if (currentBuildMatches != null && currentBuildMatchIndex < currentBuildMatches.size()) {
                int[] buildTuple = currentBuildMatches.get(currentBuildMatchIndex++);
                return concat(buildTuple, currentProbeTuple);
            }

            // fetch next probe tuple
            currentProbeTuple = probeInput.next();
            if (currentProbeTuple == null) {
                return null;
            }

            int probeKey = currentProbeTuple[probeKeyIndex];

            // streamed approximate range prefilter
            if (!streamedRanges.contains(probeKey)) {
                rangeRejects++;
                currentBuildMatches = null;
                currentBuildMatchIndex = 0;
                continue;
            }

            rangePasses++;

            // actual hash lookup
            hashLookups++;
            currentBuildMatches = hashTable.get(probeKey);
            currentBuildMatchIndex = 0;

            if (currentBuildMatches == null || currentBuildMatches.isEmpty()) {
                currentBuildMatches = null;
                continue;
            }
        }
    }

    @Override
    public void close() {
        buildInput.close();
        probeInput.close();

        hashTable.clear();
        currentProbeTuple = null;
        currentBuildMatches = null;
        currentBuildMatchIndex = 0;
    }

    private int[] concat(int[] left, int[] right) {
        int[] out = new int[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

    public int getHashLookups() {
        return hashLookups;
    }

    public int getRangePasses() {
        return rangePasses;
    }

    public int getRangeRejects() {
        return rangeRejects;
    }

    public int getApproximateRangeCount() {
        return streamedRanges != null ? streamedRanges.getRangeCount() : 0;
    }

    public long getRangeBytes() {
        return streamedRanges != null ? streamedRanges.getRangeBytes() : 0L;
    }
}