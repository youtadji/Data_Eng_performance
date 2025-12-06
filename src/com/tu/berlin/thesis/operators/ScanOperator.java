package com.tu.berlin.thesis.operators;

import java.util.List;

public class ScanOperator implements Operator {
    private List<String[]> data;
    private int currentPosition;

    public ScanOperator(List<String[]> data) {
        this.data = data;
    }

    @Override
    public void open() {
        currentPosition = 0;
        System.out.println("ScanOperator: Ready to scan " + data.size() + " rows");
    }

    @Override
    public String[] next() {
        if (currentPosition < data.size()) {
            String[] row = data.get(currentPosition);
            currentPosition++;
            return row;
        }
        return null; // No more data
    }

    @Override
    public void close() {
        System.out.println("ScanOperator: Finished scanning " + currentPosition + " rows");
    }
}