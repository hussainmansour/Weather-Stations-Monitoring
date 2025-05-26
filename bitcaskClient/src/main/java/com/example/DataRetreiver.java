package com.example;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class DataRetreiver {

    private long key;
    private String value;

    public long getKey(){
        return this.key;
    }

    public String getValue(){
        return this.value;
    }

    public void extractKeyAndValue(JsonNode node) throws IOException {
        this.key = node.get("key").asLong();
        byte[] valueArray = extractValue(node);
        this.value = deserializeRecord(valueArray);
    }

    private byte[] extractValue(JsonNode node){
        JsonNode valueArray = node.get("value");
        byte[] value = new byte[valueArray.size()];
        for (int i = 0; i < valueArray.size(); i++) {
            value[i] = (byte) valueArray.get(i).asInt();
        }
        return value;
    }

    private String deserializeRecord(byte[] data) throws IOException {
        org.apache.avro.Schema schema = new Schema.Parser().parse(new File("/Users/csed/Desktop/data-intensive/Weather-Stations-Monitoring/Central-Station/schema/messageSchema.avsc"));
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return reader.read(null, decoder).toString();
    }
}
