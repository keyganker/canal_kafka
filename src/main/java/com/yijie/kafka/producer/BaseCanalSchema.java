package com.yijie.kafka.producer;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

public class BaseCanalSchema extends Record{

	public BaseCanalSchema(Schema schema) {
		super(schema);
		// TODO Auto-generated constructor stub
	}
	private String namespace;
	private String type;
	private String name;
	private List<Map<String, Object>> fields;
	
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<Map<String, Object>> getFields() {
		return fields;
	}
	public void setFields(List<Map<String, Object>> fields) {
		this.fields = fields;
	}

}
