package server;

import static spark.Spark.*;

import com.google.gson.Gson;

public class BitcaskServer {
    public static void main(String[] args) {
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
    }
}
