package central.station;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// Parquet imports
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

// Avro imports
import org.apache.avro.generic.GenericRecord;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class WeatherDataParquetWriter {
    private final List<GenericRecord> buffer;
    private final int batchSize;
    private final String basePath;

    public WeatherDataParquetWriter(int batchSize, String basePath) {
        this.batchSize = batchSize;
        this.basePath = basePath;
        buffer = new ArrayList<>();
    }

    public void addRecords(List<GenericRecord> records) {
        buffer.addAll(records);
        if (buffer.size() >= batchSize) {
            flushBuffer();
        }
    }

    private void flushBuffer() {
        List<GenericRecord> toBeWritten = new ArrayList<>(buffer);
        buffer.clear();

        System.out.println("Flushing buffer with " + toBeWritten.size() + " records to Parquet files");

        new Thread(() -> {
            try {
                for (GenericRecord record : toBeWritten) {
                    writeParquetFile(record);
                }
                System.out.println("Successfully wrote " + toBeWritten.size() + " records to Parquet files");
            } catch (IOException e) {
                System.err.println("Error writing Parquet files: " + e.getMessage());
            }
        }).start();
    }

    private void writeParquetFile(GenericRecord record) throws IOException {
        String outputPath = String.format("%s/timestamp_%d/id_%d.parquet",
                basePath,
                ((long) record.get("status_timestamp") / 1000),
                ((Number) record.get("station_id")).intValue());

        Path path = new Path(outputPath);
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(WeatherData.getClassSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withConf(new Configuration())
                .build()) {
            writer.write(record);
        }
    }
}
