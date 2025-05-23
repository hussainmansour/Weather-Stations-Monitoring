package central.station;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class CentralStation {
    private static final String topic = "weather-data";
    private static final int BATCH_SIZE = 10_000;


    public static void main(String[] args) throws IOException {
        Properties props = getProperties();

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            int recordCount = 0;

            System.out.println("Starting to consume weather data from Kafka...");

            while (recordCount < 10) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("Consumed record: " + record);
                }
                recordCount += records.count();
            }
        }

    }

    private static void writeToParquet(List<WeatherData> buffer) throws IOException {
        String filePath = "/data/weather_" + System.currentTimeMillis() + ".parquet";
        Path path = new Path(filePath);
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
        try (ParquetWriter<WeatherData> writer = AvroParquetWriter.<WeatherData>builder(outputFile)
                .withSchema(WeatherData.getClassSchema())
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                .build()) {

            for (WeatherData data : buffer) {
                writer.write(data);
            }

        } catch (IOException e) {
            System.err.println("Error writing to Parquet file: " + filePath);
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-data-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
