package bitcask.client;

import com.google.gson.Gson;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BitcaskClient {
    private final String serverUrl;

    public BitcaskClient(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public void sendToBitcask(List<GenericRecord> records) {
        for (GenericRecord record : records) {
            add(record);
        }
    }

    private void add(GenericRecord record) {
        DataEntry entry = new DataEntry();
        byte[] bytes = null;
        try {
            bytes = serializeRecord(record);
        } catch (IOException e) {
            System.out.println("Can't serialize data to send to bitcask");
            System.err.println("Error serializing record: " + e.getMessage());
        }
        ArrayList<Byte> aa = new ArrayList<>();
        assert bytes != null;
        for (byte b : bytes)
            aa.add(b);
        entry.createEntry(Integer.parseInt(record.get("station_id").toString()), aa);
        sendDataEntry(entry);
    }

    public static byte[] serializeRecord(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        org.apache.avro.io.BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private void sendDataEntry(DataEntry dataEntry) {
        try {
            URL url = new URL(serverUrl + "/bitcask/add");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            Gson gson = new Gson();
            String jsonInputString = gson.toJson(dataEntry);

            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int code = con.getResponseCode();
            System.out.println("Response Code: " + code);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                StringBuilder response = new StringBuilder();
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println("Server response: " + response);
            }
        } catch (Exception e) {
            System.err.println("Error sending DataEntry: " + e.getMessage());
        }
    }
}
