package server;

import static spark.Spark.*;

import com.google.gson.Gson;

public class BitcaskServer {
    public static void main(String[] args) {
        port(8085); 
        Bitcask bitcask = Bitcask.getBitcaskInstance();

        get("/bitcask/view-all", (req, res) -> {
            res.type("application/json");
            return new Gson().toJson(bitcask.getAll());
        });

        get("/bitcask/view", (req, res) -> {
            long key = Long.parseLong(req.queryParams("key"));
            DataEntry entry = bitcask.get(key);
            if (entry == null) {
                res.status(404);
                return "Key not found";
            }
            res.type("application/json");
            return new Gson().toJson(entry);
        });

        Gson gson = new Gson();
        post("/bitcask/add", (req, res) -> {
            try {
                DataEntry dataEntry = gson.fromJson(req.body(), DataEntry.class);
                if (dataEntry == null) {
                    res.status(400);
                    return "Invalid DataEntry format";
                }
                bitcask.add(dataEntry);
                res.status(200);
                return "DataEntry added successfully";
            } catch (Exception e) {
                res.status(500);
                return "Error: " + e.getMessage();
            }
        });
    }
}
