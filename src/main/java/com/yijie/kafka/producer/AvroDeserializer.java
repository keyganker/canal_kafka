package com.yijie.kafka.producer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
    	Parser parser = new Parser();
		try {
			Schema mSchema = parser.parse(this.getClass().getResourceAsStream("/Members.avsc"));
	        DatumReader<T> userDatumReader = new SpecificDatumReader<>(mSchema);
	        BinaryDecoder binaryEncoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);
	        return userDatumReader.read(null, binaryEncoder);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
    }

    @Override
    public void close() {

    }
}