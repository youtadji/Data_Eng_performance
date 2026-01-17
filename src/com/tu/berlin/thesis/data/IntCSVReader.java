package com.tu.berlin.thesis.data;

import java.io.*;
import java.util.*;

public class IntCSVReader {
    public static List<int[]> readCSV(String filename) throws IOException {
        List<int[]> result = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; } // skip header
                String[] parts = line.split(",");
                int[] row = new int[parts.length];
                for (int i = 0; i < parts.length; i++) row[i] = Integer.parseInt(parts[i]);
                result.add(row);
            }
        }
        return result;
    }
}
