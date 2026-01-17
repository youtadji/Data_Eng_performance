package com.tu.berlin.thesis.rangetree;

import java.util.Arrays;

/**
 * Exact range index implemented as a bulk-loaded B+Tree over ranges.
 *
 * Leaves store actual ranges as (start,end) arrays.
 * Internal nodes store separator "start" keys for navigation.
 *
 * Query: contains(key) is true iff key is inside ANY stored range.
 *
 * This follows the B-tree search idea from the indexing lecture:
 * multiway nodes + binary search within nodes. :contentReference[oaicite:2]{index=2}
 */
public final class RangeBPlusTree {

    // Tune these if needed (fanout & leaf capacity).
    // Higher values -> smaller height, fewer pointer hops.
    private static final int LEAF_CAP = 128;   // ranges per leaf
    private static final int FANOUT   = 128;   // children per internal node

    private abstract static class Node {
        final boolean leaf;
        Node(boolean leaf) { this.leaf = leaf; }
    }

    /** Leaf nodes hold the actual ranges. */
    private static final class Leaf extends Node {
        final int[] starts;
        final int[] ends;
        final int count;

        Leaf(int[] starts, int[] ends, int count) {
            super(true);
            this.starts = starts;
            this.ends = ends;
            this.count = count;
        }
    }

    /** Internal nodes hold separator starts and child pointers. */
    private static final class Internal extends Node {
        final int[] sepStarts;   // length = childCount - 1
        final Node[] children;   // length = childCount
        final int childCount;

        Internal(int[] sepStarts, Node[] children, int childCount) {
            super(false);
            this.sepStarts = sepStarts;
            this.children = children;
            this.childCount = childCount;
        }
    }

    private Node root;
    private int rangeCount;
    private long approxBytes; // rough bytes for stored arrays (not object headers)

    public RangeBPlusTree() {
        this.root = null;
        this.rangeCount = 0;
        this.approxBytes = 0;
    }

    /**
     * Bulk-load from sorted ranges (by start).
     * starts[i], ends[i] define one range; count is number of ranges.
     */
    public void buildFromRanges(int[] starts, int[] ends, int count) {
        this.rangeCount = count;
        this.approxBytes = 0;

        if (count == 0) {
            root = null;
            return;
        }

        // -----------------------
        // we Build leaf  nodes  level firstt
        // -----------------------
        int leafCount = (count + LEAF_CAP - 1) / LEAF_CAP;
        Node[] level = new Node[leafCount];

        int pos = 0;
        for (int i = 0; i < leafCount; i++) {
            int len = Math.min(LEAF_CAP, count - pos);

            // copy exactly the ranges for this leaf
            int[] ls = Arrays.copyOfRange(starts, pos, pos + len);  //pos = where we are in starts[]/ends[]


            int[] le = Arrays.copyOfRange(ends, pos, pos + len); //len = how many ranges go into this leaf

            level[i] = new Leaf(ls, le, len);

            // approx bytes: two int arrays
            approxBytes += (long) len * 4L * 2L;

            pos += len;
        }

        // -----------------------
        //  Build internal levels
        // until one root remains
        // -----------------------
        while (level.length > 1) {
            int parentCount = (level.length + FANOUT - 1) / FANOUT;
            Node[] next = new Node[parentCount];

            int idx = 0;
            for (int p = 0; p < parentCount; p++) {
                int childCount = Math.min(FANOUT, level.length - idx);

                Node[] children = new Node[childCount];
                for (int c = 0; c < childCount; c++) {
                    children[c] = level[idx + c];
                }

                // separator keys = first start of each child except first
                int[] seps = new int[Math.max(0, childCount - 1)];
                for (int s = 1; s < childCount; s++) {
                    seps[s - 1] = firstStart(children[s]);
                }

                next[p] = new Internal(seps, children, childCount);

                // approx bytes: separator ints
                approxBytes += (long) seps.length * 4L;

                idx += childCount;
            }

            level = next;
        }

        root = level[0];
    }

    /**
     * Exact membership: true iff key is inside any stored range.
     * Probe-time hot path: no allocations.
     */
    public boolean contains(int key) {
        if (root == null) return false;

        Node n = root;

        // descend internal nodes
        while (!n.leaf) {
            Internal in = (Internal) n;

            // choose child using upperBound over separator starts
            int childIndex = upperBound(in.sepStarts, in.childCount - 1, key);
            n = in.children[childIndex];
        }

        // leaf lookup: last start <= key
        Leaf leaf = (Leaf) n;
        int i = upperBound(leaf.starts, leaf.count, key) - 1;
        if (i < 0) return false;

        return key <= leaf.ends[i];
    }

    public int getRangeCount() {
        return rangeCount;
    }

    public long approxBytesUsed() {
        return approxBytes;
    }

    // -----------------------
    // Helper methods
    // -----------------------

    /**
     * upperBound: first index where arr[idx] > key, in range [0..len]
     * If all arr <= key, returns len.
     */
    private static int upperBound(int[] arr, int len, int key) {
        int lo = 0;
        int hi = len;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] <= key) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** Get the first start value contained in a node (go down to leftmost leaf). */
    private static int firstStart(Node node) {
        Node n = node;
        while (!n.leaf) {
            n = ((Internal) n).children[0];
        }
        return ((Leaf) n).starts[0];
    }
}
