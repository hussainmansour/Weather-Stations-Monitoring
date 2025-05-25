package com.example;

import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;

public class CsvWriter {
    public static void writeToFile(HttpURLConnection conn, int threadId) throws Exception {
        Parser parser = new Parser();
        ;
        DataRetreiver dataRetreiver = new DataRetreiver();
        ;
        ArrayList<JsonNode> jsonNodes = parser.extractData(conn);
        ArrayList<Long> keys = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        for (JsonNode jsonNode : jsonNodes) {
            dataRetreiver.extractKeyAndValue(jsonNode);
            keys.add(dataRetreiver.getKey());
            values.add(dataRetreiver.getValue());
        }
        writeKeyValueToCsv(keys, values, threadId);
    }

    private static void writeKeyValueToCsv(ArrayList<Long> key, ArrayList<String> value, int threadId) {
        String filename = Instant.now().toEpochMilli() + ".csv";
        if(threadId != -1)
            filename = Instant.now().toEpochMilli()+ "thread-" + threadId + ".csv";

        try (FileWriter writer = new FileWriter(filename)) {
            for (int i = 0; i < key.size(); i++)
                writer.write(key.get(i) + "," + value.get(i) + "\n");
            System.out.println("Written to: " + filename);
        } catch (IOException e) {
            System.err.println("Error writing CSV: " + e.getMessage());
        }
    }
}
