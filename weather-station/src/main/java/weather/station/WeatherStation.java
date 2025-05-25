package weather.station;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class WeatherStation {
    private static final double lowBatteryPercentage = 0.3;
    private static final double mediumBatteryPercentage = 0.4;
    private static final String topic = "weather-data";

    public static void main(String[] args) {
        Properties props = getProps();
        long stationId = Long.parseLong(System.getenv("STATION_ID"));
        long seq = 0;
        Random rand = new Random();
        Schema schema = WeatherData.getClassSchema();

        System.out.println("Sending weather data to Kafka...");

        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (true) {
                GenericRecord status = generateStatusRecord(stationId, seq++, schema);
                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, String.valueOf(stationId), status);
                sendStatus(producer, record, rand);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendStatus(Producer<String, GenericRecord> producer, ProducerRecord<String, GenericRecord> record,
                                   Random rand) {
        if (rand.nextInt(10) != 5) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to produce: " + exception.getMessage());
                }
            });
            System.out.println("Sent record: " + record);
        }
    }

    private static Properties getProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
        return props;
    }

    private static GenericRecord generateStatusRecord(long stationId, long seq, Schema schema) {
        Random rand = new Random();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("station_id", stationId);
        genericRecord.put("s_no", seq);
        genericRecord.put("battery_status", getBatteryStatus(rand));
        genericRecord.put("status_timestamp", System.currentTimeMillis());
        genericRecord.put("weather", getWeather(rand));
        return genericRecord;
    }

    private static BatteryStatus getBatteryStatus(Random rand) {
        double randomValue = rand.nextDouble();
        if (randomValue <= lowBatteryPercentage)
            return BatteryStatus.low;
        if (randomValue <= mediumBatteryPercentage)
            return BatteryStatus.medium;
        return BatteryStatus.high;
    }

    private static Weather getWeather(Random rand) {
        return Weather.newBuilder()
                .setHumidity(rand.nextInt(100))
                .setTemperature(rand.nextInt(50))
                .setWindSpeed(rand.nextInt(100))
                .build();
    }
}
