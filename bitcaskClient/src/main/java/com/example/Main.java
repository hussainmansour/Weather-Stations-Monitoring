package com.example;

import java.net.HttpURLConnection;
import java.net.URL;

public class Main {
    private static final String SERVER_URL = "http://localhost:8085";

    public static void main(String[] args) {
        viewAll();
        view(1);
    }

    public static void viewAll() {
        try {
            URL url = new URL(SERVER_URL + "/bitcask/view-all");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            CsvWriter.writeToFile(conn);
        } catch (Exception e) {
            System.out.println("Error while calling viewAll: " + e.getMessage());
        }
    }

    public static void view(long key) {
        try {
            URL url = new URL(SERVER_URL + "/bitcask/view?key=" + key);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            if (status == 404) {
                System.out.println("Key not found.");
                return;
            }
            CsvWriter.writeToFile(conn);
        } catch (Exception e) {
            System.out.println("Error while calling view: " + e.getMessage());
        }
    }
}