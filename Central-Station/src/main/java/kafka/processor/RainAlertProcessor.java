package kafka.processor;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;

public class RainAlertProcessor {
    private static KafkaStreams streams;

    public static void start() {
        // This method will be implemented to start the Rain Alert Processor
        // It will handle the processing of rain alerts from Kafka topics
        System.out.println("Rain Alert Processor started.");

        final Map<String, String> serdeConfig = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                System.getenv("SCHEMA_REGISTRY_URL")
        );

        final StreamsBuilder builder = new StreamsBuilder();
        final GenericAvroSerde valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);

        KStream<String, GenericRecord> stream = builder.stream("weather-data",
                Consumed.with(Serdes.String(), valueSerde));

        stream.peek((key, value) -> System.out.println("Received record: key=" + key + ", value=" + value))
              .filter((key, value) -> {
                  GenericRecord weather = (GenericRecord) value.get("weather");
                  int humidityValue = (Integer) weather.get("humidity");
                  return humidityValue > 70;
              })
              .mapValues(value -> {
                  String stationId = value.get("station_id").toString();
                    GenericRecord weather = (GenericRecord) value.get("weather");
                    int humidityValue = (Integer) weather.get("humidity");
                    String alertMessage = "Rain Alert for Station ID: " + stationId + ", Humidity: " + humidityValue + "%";
                    System.out.println("Generated alert: " + alertMessage);
                    return alertMessage;
              }).to("rain-alerts", Produced.with(Serdes.String(), Serdes.String()));

        streams = new KafkaStreams(builder.build(), getProperties());
        streams.setUncaughtExceptionHandler((t) -> {
            System.err.println("Stream error in thread " + t.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        System.out.println("Rain Alert Processor is running...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Rain Alert Processor...");
            streams.close();
        }));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-alert-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
