package com.tu.berlin.thesis.rangetree;

/**
 * Exact membership using only two arrays (starts[], ends[]).
 * Ranges must be sorted and non-overlapping (RangeExtractor guarantees this).
 *
 * contains(key):
 *  - find i = last index where starts[i] <= key
 *  - check key <= ends[i]
 */
public final class ExactRangesIndex {

    private int[] starts = new int[0];
    private int[] ends = new int[0];
    private int count = 0;

    public void build(int[] starts, int[] ends, int count) {
        this.starts = starts;
        this.ends = ends;
        this.count = count;
    }

    public boolean contains(int key) {
        if (count == 0) return false;

        // upperBound(starts, count, key) - 1
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (starts[mid] <= key) lo = mid + 1;
            else hi = mid;
        }
        int i = lo - 1;
        return i >= 0 && key <= ends[i];
    }

    public int getRangeCount() { return count; }

    public long approxBytesUsed() {
        // starts + ends arrays (ints), ignoring object headers
        return (long) count * 2L * 4L;
    }
}
