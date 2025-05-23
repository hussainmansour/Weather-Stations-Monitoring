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
    private static final int noOfRecords = 10_000;
    private static final double lowBatteryPercentage = 0.3;
    private static final double mediumBatteryPercentage = 0.4;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));

        String topic = "weather-data";

        int lowBatteryCount = (int) (noOfRecords * lowBatteryPercentage);
        int mediumBatteryCount = (int) (noOfRecords * mediumBatteryPercentage);
        int highBatteryCount = noOfRecords - lowBatteryCount - mediumBatteryCount;

        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            Random rand = new Random();
            long stationId = 1L;
            long seq = 0;

            int loops = noOfRecords;

            System.out.println("Sending weather data to Kafka...");

            Schema schema = WeatherData.getClassSchema();

            while (loops-- > 0) {
                boolean validBattery = false;
                int batteryStatus = rand.nextInt(3);
                while (!validBattery) {
                    if (batteryStatus == 0 && lowBatteryCount > 0) {
                        lowBatteryCount--;
                        validBattery = true;
                    } else if (batteryStatus == 1 && mediumBatteryCount > 0) {
                        mediumBatteryCount--;
                        validBattery = true;
                    } else if (batteryStatus == 2 && highBatteryCount > 0) {
                        highBatteryCount--;
                        validBattery = true;
                    } else {
                        batteryStatus = (batteryStatus + rand.nextInt(1, 3)) % 3;
                    }
                }


                GenericRecord genericRecord = new GenericData.Record(schema);
                genericRecord.put("station_id", stationId);
                genericRecord.put("s_no", seq++);
                genericRecord.put("battery_status", batteryStatus == 0 ? BatteryStatus.low : batteryStatus == 1 ?
                        BatteryStatus.medium : BatteryStatus.high);
                genericRecord.put("status_timestamp", System.currentTimeMillis());
                genericRecord.put("weather", Weather.newBuilder().setHumidity(rand.nextInt()).setTemperature(rand.nextInt())
                        .setWindSpeed(rand.nextInt()).build());

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, String.valueOf(stationId), genericRecord);
                boolean send = rand.nextInt(10) != 5;

                if (send) {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to produce: " + exception.getMessage());
                        }
                    });
                    System.out.println("Sent record: " + record);
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Weather data sent to Kafka topic: " + topic);
    }
}
