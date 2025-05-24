package com.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.google.gson.Gson;

public class BitcaskClient {

    public static void sendDataEntry(String serverUrl, DataEntry dataEntry) {
        try {
            URL url = new URL(serverUrl + "/bitcask/add");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            Gson gson = new Gson();
            String jsonInputString = gson.toJson(dataEntry);

            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int code = con.getResponseCode();
            System.out.println("Response Code: " + code);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
                String responseLine;
                StringBuilder response = new StringBuilder();
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println("Server response: " + response.toString());
            }
        } catch (Exception e) {
            System.err.println("Error sending DataEntry: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        long x = 0;
        // while (System.currentTimeMillis() - start < 900_000) {
        for(int j = 0; j < 10; j++){
            for (int i = 0; i < 1; i++) {
                add(x++);
            }
        }
    }

    public static void add(long i) {
        DataEntry entry = new DataEntry();
        byte[] a = ByteBuffer.allocate(Long.BYTES).putLong(i).array();
        ArrayList<Byte> aa = new ArrayList<>();
        for (byte b : a)
            aa.add(b);
        entry.createEntry(i % 3, aa);
        sendDataEntry("http://localhost:8085", entry);
    }
}
