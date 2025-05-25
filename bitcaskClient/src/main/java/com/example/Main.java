package com.example;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final String SERVER_URL = "http://localhost:8085";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage:\n" +
                    "  --view-all\n" +
                    "  --view --key=SOME_KEY\n" +
                    "  --perf --clients=N");
            return;
        }

        switch (args[0]) {
            case "--view-all":
                viewAll();
                break;
            case "--view":
                long key = parseKeyArg(args);
                if (key != -1) view(key);
                else System.out.println("Missing or invalid key argument.");
                break;
            case "--perf":
                int clients = parseClientsArg(args);
                if (clients > 0) performanceTest(clients);
                else System.out.println("Missing or invalid client count.");
                break;
            default:
                System.out.println("Unknown command: " + args[0]);
        }
    }

    public static void viewAll() {
        try {
            URL url = new URL(SERVER_URL + "/bitcask/view-all");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            CsvWriter.writeToFile(conn, -1); // no threadId
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
            CsvWriter.writeToFile(conn, -1); 
        } catch (Exception e) {
            System.out.println("Error while calling view: " + e.getMessage());
        }
    }

    public static void performanceTest(int n) {
        ExecutorService executor = Executors.newFixedThreadPool(n);
        for (int i = 1; i <= n; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    URL url = new URL(SERVER_URL + "/bitcask/view-all");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");
                    CsvWriter.writeToFile(conn, threadId);
                } catch (Exception e) {
                    System.out.println("Thread " + threadId + " error: " + e.getMessage());
                }
            });
        }
        executor.shutdown();
    }

    private static long parseKeyArg(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--key=")) {
                try {
                    return Long.parseLong(arg.substring("--key=".length()));
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private static int parseClientsArg(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--clients=")) {
                try {
                    return Integer.parseInt(arg.substring("--clients=".length()));
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }
}
