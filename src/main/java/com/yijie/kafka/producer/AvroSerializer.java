package com.yijie.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerializer<T extends Record> implements Serializer<Record>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Record data) {
        DatumWriter<Record> userDatumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            userDatumWriter.write(data, binaryEncoder);
        } catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
        return outputStream.toByteArray();
    }

    @Override
    public void close() {

    }
}