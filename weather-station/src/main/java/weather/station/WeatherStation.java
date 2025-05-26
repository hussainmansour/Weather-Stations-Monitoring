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

import java.util.*;

public class WeatherStation {
    private static final String topic = "weather-data";
    private static final List<Integer> statusPool = new ArrayList<>(100);
    private static final Random random = new Random();

    public static void main(String[] args) {
        Properties props = getProps();
        String podName = System.getenv("STATION_ID");
        long stationId = Long.parseLong(podName.substring(podName.lastIndexOf("-") + 1));

        long seq = 0;
        int idx = 0;
        Schema schema = WeatherData.getClassSchema();
        fillStatusPool();

        System.out.println("Sending weather data to Kafka...");

        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (true) {
                if (idx >= statusPool.size()) {
                    Collections.shuffle(statusPool);
                    idx = 0;
                }

                int messageStatus = statusPool.get(idx++);

                if (messageStatus == 0) {
                    System.out.println("Dropped message for station " + stationId + " at sequence " + seq);
                } else {
                    GenericRecord status = generateStatusRecord(stationId, seq++, schema, messageStatus);
                    ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, String.valueOf(stationId), status);
                    sendStatus(producer, record);
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendStatus(Producer<String, GenericRecord> producer, ProducerRecord<String, GenericRecord> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to produce: " + exception.getMessage());
            }
        });
        System.out.println("Sent record: " + record);
    }

    private static Properties getProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
        return props;
    }

    private static GenericRecord generateStatusRecord(long stationId, long seq, Schema schema, int batteryStatus) {
        Random rand = new Random();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("station_id", stationId);
        genericRecord.put("s_no", seq);
        genericRecord.put("battery_status", getBatteryStatus(batteryStatus));
        genericRecord.put("status_timestamp", System.currentTimeMillis());
        genericRecord.put("weather", getWeather(rand));
        return genericRecord;
    }

    private static BatteryStatus getBatteryStatus(int status) {
        return (status == 1) ? BatteryStatus.low :
                (status == 2) ? BatteryStatus.medium :
                BatteryStatus.high;
    }

    private static Weather getWeather(Random rand) {
        return Weather.newBuilder()
                .setHumidity(rand.nextInt(100))
                .setTemperature(rand.nextInt(50))
                .setWindSpeed(rand.nextInt(100))
                .build();
    }

    private static void fillStatusPool() {
        for (int i = 0; i < 10; i++) statusPool.add(0); // 10% dropped messages
        for (int i = 0; i < 27; i++) statusPool.add(1); // 30% low battery
        for (int i = 0; i < 27; i++) statusPool.add(3); // 30% high battery
        for (int i = 0; i < 36; i++) statusPool.add(2); // 40% medium battery
        Collections.shuffle(statusPool, random);
    }
}
