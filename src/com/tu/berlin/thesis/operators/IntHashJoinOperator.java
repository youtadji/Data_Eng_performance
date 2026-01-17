package com.tu.berlin.thesis.operators;

import java.util.*;

public class IntHashJoinOperator implements IntOperator {

    private final IntOperator leftOp;
    private final IntOperator rightOp;
    private final int leftKeyIndex;
    private final int rightKeyIndex;

    private final Map<Integer, List<int[]>> hashTable = new HashMap<>();

    private int[] currentRightRow;
    private Iterator<int[]> matchIterator;

    // metrics
    private int hashLookups = 0;
    private int actualMatches = 0;

    public int getHashLookups() { return hashLookups; }
    public int getActualMatches() { return actualMatches; }

    public IntHashJoinOperator(IntOperator left, IntOperator right, int leftKeyIndex, int rightKeyIndex) {
        this.leftOp = left;
        this.rightOp = right;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
    }

    @Override
    public void open() {
        // BUILD
        leftOp.open();
        int[] leftRow;

        while ((leftRow = leftOp.next()) != null) {
            int key = leftRow[leftKeyIndex];

            List<int[]> tmp = hashTable.get(key); // one lookup
            if (tmp == null) {
                tmp = new ArrayList<>();
                hashTable.put(key, tmp);
            }
            tmp.add(leftRow);
        }
        leftOp.close();

        // PROBE prepare
        rightOp.open();
        advanceToNextMatch();
    }
    //tries to find the next right row that actually has matches
    private void advanceToNextMatch() {
        matchIterator = null;

        while (matchIterator == null || !matchIterator.hasNext()) {

            currentRightRow = rightOp.next();
            if (currentRightRow == null) return;

            int key = currentRightRow[rightKeyIndex];

            hashLookups++;
            List<int[]> matches = hashTable.get(key);

            if (matches != null) {
                matchIterator = matches.iterator();
                actualMatches += matches.size();
            }
        }
    }

    @Override
    public int[] next() {
        while (true) {
            if (currentRightRow == null) return null;

            if (matchIterator != null && matchIterator.hasNext()) {
                int[] leftRow = matchIterator.next();

                int[] out = new int[leftRow.length + currentRightRow.length];
                System.arraycopy(leftRow, 0, out, 0, leftRow.length);
                System.arraycopy(currentRightRow, 0, out, leftRow.length, currentRightRow.length);

                if (!matchIterator.hasNext()) advanceToNextMatch();
                return out;
            }

            advanceToNextMatch();
        }
    }

    @Override
    public void close() {
        rightOp.close();
        hashTable.clear();
    }
}
