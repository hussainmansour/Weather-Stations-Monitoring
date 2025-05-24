package central.station;

// Kafka imports
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.Gson;

import org.apache.avro.generic.GenericDatumWriter;
// Avro imports
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.codec.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.Schema;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Flushable;
// Java imports
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class CentralStation {
    private static final String topic = "weather-data";

    public static void main(String[] args) throws IOException {
        Properties props = getProperties();
        List<GenericRecord> buffer = new ArrayList<>();
        WeatherDataParquetWriter parquetWriter = new WeatherDataParquetWriter(10_000, "data");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Starting to consume weather data from Kafka...");

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    buffer.add(record.value());
                }
                for (GenericRecord record : buffer) {
                    add(record);
                }
                parquetWriter.addRecords(buffer);

            }
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-data-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void add(GenericRecord record) {
        DataEntry entry = new DataEntry();
        byte[] bytes = genericRecordToBytes(record, record.getSchema());
        ArrayList<Byte> aa = new ArrayList<>();
        for (byte b : bytes)
            aa.add(b);
        entry.createEntry(Integer.parseInt(record.get("station_id").toString()), aa);
        sendDataEntry("http://localhost:8085", entry);
    }

    public static byte[] genericRecordToBytes(GenericRecord record, Schema schema) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = (org.apache.commons.codec.BinaryEncoder) EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(record, (Encoder) encoder);
            ((Flushable) encoder).flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    private static void sendDataEntry(String serverUrl, DataEntry dataEntry) {
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

}
