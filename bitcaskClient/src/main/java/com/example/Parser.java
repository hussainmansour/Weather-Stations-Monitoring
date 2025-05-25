package com.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Parser {

    private ArrayList<JsonNode> jsonNodes;

    public Parser(){
        jsonNodes = new ArrayList<>();
    }

    public ArrayList<JsonNode> extractData(HttpURLConnection conn) throws Exception{
        String response = extractResponse(conn);
        return parseResponse(response);
    }

    private String extractResponse(HttpURLConnection conn) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        return response.toString();
    }

    private ArrayList<JsonNode> parseResponse(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        if (root.isArray()) {
            for (JsonNode node : root) {
                jsonNodes.add(node);
            }
        } else {
            jsonNodes.add(root);
        }
        return jsonNodes;
    }
    
}
