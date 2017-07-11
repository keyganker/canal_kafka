package com.yijie.kafka.producer;

import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.util.*;

public class YijieKafkaProducer {

	protected final static Logger logger = LoggerFactory.getLogger(YijieKafkaProducer.class);
	private Gson gson = new Gson();
	private Yaml yaml = new Yaml();
	private String format = "json";
	private String topicPrefix = "canal-";
	private HashMap<String, String> topics = new HashMap<String, String>();
	@SuppressWarnings("rawtypes")
	private Producer producer = null;
	private Map<String, Map<String, Object>> blacklist = null;


	@SuppressWarnings("rawtypes")
	public YijieKafkaProducer(String format, Properties properties) {
		this.format = format;
		if ("json".equals(this.format)) {
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		}
		else if ("avro".equals(this.format)) {
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		}
		this.producer = new KafkaProducer(properties);
        try {
            if (properties.containsKey("blacklist")) {
                this.blacklist = (Map<String, Map<String, Object>>) yaml.load(new FileInputStream(new File((String) properties.get("blacklist"))));
            } else {
                this.blacklist = (Map<String, Map<String, Object>>) yaml.load(new FileInputStream(new File(YijieKafkaProducer.class.getClassLoader().getResource("blacklist.yaml").getPath())));
            }
        } catch (FileNotFoundException e) {
            logger.warn("Load blacklist fail", e);
            System.exit(10004);
        }


	}

	public void close() {
		this.producer.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> void send(Entry entry) {
		RowChange rowChage = null;
		try {
			rowChage = RowChange.parseFrom(entry.getStoreValue());
		} catch (Exception e) {
			throw new RuntimeException("parse event has an error, data: " + entry.toString(), e);
		}
		for (RowData rowData : rowChage.getRowDatasList()) {
			T message = this.convert(entry, rowData);
			String schemaName = entry.getHeader().getSchemaName();
			String topicName;
			if (topics.containsKey(schemaName)) {
				topicName = topics.get(schemaName);
			} else {
				topicName = topicPrefix + schemaName;
				topics.put(schemaName, topicName);
			}
			this.producer.send(new ProducerRecord(topicName, message));
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T convert(Entry entry, RowData rowData) {
		long executeTime = entry.getHeader().getExecuteTime() / 1000;
		String dbName = entry.getHeader().getSchemaName();
		String tableName = entry.getHeader().getTableName();
		RowChange rowChage = null;
		try {
			rowChage = RowChange.parseFrom(entry.getStoreValue());
		} catch (Exception e) {
			throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
		}

		EventType eventType = rowChage.getEventType();
		if ("json".equals(this.format)) {
			return (T) this.convertToJson(eventType, dbName, tableName, executeTime, rowData);
		} else if ("avro".equals(this.format)) {
			return (T) this.convertToAvro(eventType, dbName, tableName, executeTime, rowData);
		} else {
			logger.info("Do not support the format[{}]", this.format);
			throw new RuntimeException("Message type not support, type: " + this.format);
		}
	}

	public String convertToJson(EventType eventType, String dbName, String tableName, long executeTime, RowData rowData) {
		Map<String, String> beforeChange = new HashMap<String, String>();
		Map<String, String> afterChange = new HashMap<String, String>();
		Map<String, Object> message = new HashMap<String, Object>();
        List<String> blockFields = this.getBlockFields(dbName, tableName);
        int blockFieldsCount = blockFields.size();
		List<Column> afterInsertColumns = rowData.getAfterColumnsList();
		message.put("executeTime", executeTime);
		if (eventType == EventType.INSERT) {
			for (Column column : afterInsertColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    afterChange.put(column.getName(), column.getValue());
                }
			}
		} else if (eventType == EventType.UPDATE) {
			List<Column> beforeUpdateColumns = rowData.getBeforeColumnsList();
			List<Column> afterUpdateColumns = rowData.getAfterColumnsList();
			for (Column column : beforeUpdateColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    beforeChange.put(column.getName(), column.getValue());
                }
			}

			for (Column column : afterUpdateColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    afterChange.put(column.getName(), column.getValue());
                }
			}
		} else if (eventType == EventType.DELETE) {
			List<Column> beforeDeleteColumns = rowData.getBeforeColumnsList();
			for (Column column : beforeDeleteColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    beforeChange.put(column.getName(), column.getValue());
                }
			}
		}
		message.put("dbname", dbName);
		message.put("tablename", tableName);
		message.put("op", eventType.toString());
		message.put("beforeChange", beforeChange);
		message.put("data", afterChange);

		return gson.toJson(message);
	}

	public byte[] convertToAvro(EventType eventType, String dbName, String tableName, long executeTime, RowData rowData) {
		AvroSchemaBuilder asb = new AvroSchemaBuilder();
		List<Column> afterInsertColumns = rowData.getAfterColumnsList();
		GenericRecord updateRecord = null;
		List<String> blockFields = this.getBlockFields(dbName, tableName);
		int blockFieldsCount = blockFields.size();
		if (eventType == EventType.INSERT) {
			updateRecord = new GenericData.Record(asb.build("data", tableName, afterInsertColumns).export());
			for (Column column : afterInsertColumns) {
			    if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    updateRecord.put(column.getName(), column.getValue());
                }
			}
		} else if (eventType == EventType.UPDATE) {
			List<Column> beforeUpdateColumns = rowData.getBeforeColumnsList();
			List<Column> afterUpdateColumns = rowData.getAfterColumnsList();
			asb.build("beforeChange", tableName, beforeUpdateColumns);
			asb.build("data", tableName, afterUpdateColumns);
			updateRecord = new GenericData.Record(asb.export());

			GenericRecord beforeUpdateRecord = new GenericData.Record(asb.buildFieldSchema(tableName,
					beforeUpdateColumns));
			for (Column column : beforeUpdateColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    beforeUpdateRecord.put(column.getName(), column.getValue());
                }
			}

			GenericRecord afterUpdateRecord = new GenericData.Record(
					asb.buildFieldSchema(tableName, afterUpdateColumns));
			for (Column column : afterUpdateColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    afterUpdateRecord.put(column.getName(), column.getValue());
                }
			}
			updateRecord.put("beforeChange", beforeUpdateRecord);
			updateRecord.put("data", afterUpdateRecord);
		} else if (eventType == EventType.DELETE) {
			List<Column> beforeDeleteColumns = rowData.getBeforeColumnsList();
			updateRecord = new GenericData.Record(asb.build("beforeChange", tableName, beforeDeleteColumns).export());
			for (Column column : beforeDeleteColumns) {
                if (blockFieldsCount == 0 || !blockFields.contains(column.getName())) {
                    updateRecord.put(column.getName(), column.getValue());
                }
			}
		}
		updateRecord.put("executeTime", String.valueOf(executeTime));
		updateRecord.put("dbname", dbName);
		updateRecord.put("tablename", tableName);
		updateRecord.put("op", eventType.name());
		DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>(updateRecord.getSchema());
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);
		byte[] avroBytes = null;
		try {
			dataFileWriter.create(updateRecord.getSchema(), outputStream);
			dataFileWriter.append(updateRecord);
			dataFileWriter.close();
			avroBytes = outputStream.toByteArray();

//			反序列化
//			SeekableByteArrayInput sba = new SeekableByteArrayInput(avroBytes);
//			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
//			try {
//				DataFileReader<GenericRecord> dfr = new DataFileReader<GenericRecord>(sba, datumReader);
//				GenericRecord payload = null;
//				while (dfr.hasNext()) {
//					// Reuse user object by passing it to next(). This saves us
//					// from
//					// allocating and garbage collecting many objects for files
//					// with
//					// many items.
//					payload = dfr.next(payload);
//				}
//				dfr.close();
//			} catch (IOException e1) {
//				logger.error("Error while serialize kafka message, eventType[{}] dbName[{}] tableName[{}] executeTime[{}]", eventType.toString(), dbName, tableName, executeTime);
//				throw new SerializationException(e1);
//			}
		} catch (IOException e) {
			logger.error("Error while serialize kafka message, eventType[{}] dbName[{}] tableName[{}] executeTime[{}]", eventType.toString(), dbName, tableName, executeTime);
			throw new SerializationException(e);
		}

		return avroBytes;
	}

	private List<String> getBlockFields(String dbName, String tableName) {
        Map<String, Object> blockTables = this.blacklist.get(dbName);
        if (blockTables != null) {
            if ( blockTables.containsKey(tableName)) {
                return (List<String>) blockTables.get(tableName);
            }
        }

        return new ArrayList<String>();
    }
}
