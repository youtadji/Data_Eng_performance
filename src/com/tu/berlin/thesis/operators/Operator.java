package com.tu.berlin.thesis.operators;

public interface Operator {
    void open();      // Get ready to work
    String[] next();  // Give me the next row,kinda producing next row of output
    void close();     // I'm done, clean up
}