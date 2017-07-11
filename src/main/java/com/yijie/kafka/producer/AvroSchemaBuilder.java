package com.yijie.kafka.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class AvroSchemaBuilder {

	private Gson gson = new Gson();
	private BaseCanalSchema baseSchema = null;
	private List<Map<String, Object>> fields = null;

	public AvroSchemaBuilder() {
		try {
			File file = new File(Test.class.getClassLoader().getResource("canal.avsc").getPath());
			InputStream input = new FileInputStream(file);
			JsonReader reader = new JsonReader(new InputStreamReader(input, "UTF-8"));
			baseSchema = gson.fromJson(reader, BaseCanalSchema.class);
			fields = baseSchema.getFields();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public AvroSchemaBuilder build(String name, String tableName, List<Column> cloumns) {
		Map<String, Object> outerField = new HashMap<String, Object>();
		outerField.put("name", name);
		
		Map<String, Object> interRecord = new HashMap<String, Object>();
		List<Map<String, String>> interFields = new ArrayList<Map<String, String>>();
		interRecord.put("type", "record");
		interRecord.put("name", name + "." + tableName);
		for (Column column : cloumns) {
			Map<String, String> interField = new HashMap<String, String>();
			interField.put("name", column.getName());
			interField.put("type", "string");
			interFields.add(interField);
		}
		interRecord.put("fields", interFields);
		outerField.put("type", interRecord);
		fields.add(outerField);
		
		return this;
	}
	
	public Schema buildFieldSchema(String tableName, List<Column> cloumns)
	{
		Map<String, Object> record = new HashMap<String, Object>();
		record.put("namespace", baseSchema.getNamespace());
		record.put("name", tableName);
		record.put("type", "record");
		
		List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
		for (Column column : cloumns) {
			Map<String, String> field = new HashMap<String, String>();
			field.put("name", column.getName());
			field.put("type", "string");
			fields.add(field);
		}
		record.put("fields", fields);
		
		return new Schema.Parser().parse(gson.toJson(record));
	}
	
	public Schema export()
	{
		baseSchema.setFields(fields);
		return new Schema.Parser().parse(gson.toJson(baseSchema));
	}
}
