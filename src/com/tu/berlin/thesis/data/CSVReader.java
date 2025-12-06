package com.tu.berlin.thesis.data;

import java.io.*;
import java.util.*;

public class CSVReader {
    public static List<String[]> readCSV(String filename) throws IOException {
        List<String[]> result = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;

        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            result.add(values);
        }

        reader.close();
        return result;
    }

    public static void main(String[] args) {
        try {
            List<String[]> students = readCSV("data/students.csv");

            System.out.println("I read " + students.size() + " students:");
            for (String[] student : students) {
                System.out.println("ID: " + student[0] +
                        ", Name: " + student[1] +
                        ", Age: " + student[2]);
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}