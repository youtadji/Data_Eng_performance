package com.tu.berlin.thesis.operators;

import java.util.List;

public class IntScanOperator implements IntOperator {
    private final List<int[]> data;
    private int pos;

    public IntScanOperator(List<int[]> data) { this.data = data; }

    @Override public void open() { pos = 0; }

    @Override public int[] next() {
        return (pos < data.size()) ? data.get(pos++) : null;
    }

    @Override public void close() { }
}
