package central.station;

// Kafka imports
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.processor.RainAlertProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import bitcask.client.BitcaskClient;

// Avro imports
import org.apache.avro.generic.GenericRecord;
// Java imports
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CentralStation {
    private static final String topic = "weather-data";

    public static void main(String[] args) {
        System.out.println("-------------------------------- New Image --------------------------------");
        System.out.println("Starting Central Station application...");
        System.out.println("Initializing Rain Alert Processor...");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(RainAlertProcessor::start);

        System.out.println("Configuring Kafka consumer...");
        Properties props = getProperties();
        System.out.println("Kafka bootstrap servers: " + System.getenv("BOOTSTRAP_SERVERS"));
        System.out.println("Schema Registry URL: " + System.getenv("SCHEMA_REGISTRY_URL"));
        
        List<GenericRecord> buffer = new ArrayList<>();
        System.out.println("Initializing Parquet writer with batch size: 10,000");
        WeatherDataParquetWriter parquetWriter = new WeatherDataParquetWriter(50, "data");
        BitcaskClient bitcaskClient = new BitcaskClient("http://bitcask:8085");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            System.out.println("Subscribing to topic: " + topic);
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Starting to consume weather data from Kafka...");

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println("Received " + records.count() + " records in this batch");

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    buffer.add(record.value());
                }

                parquetWriter.addRecords(buffer);
                bitcaskClient.sendToBitcask(buffer);
                buffer.clear();

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



}
