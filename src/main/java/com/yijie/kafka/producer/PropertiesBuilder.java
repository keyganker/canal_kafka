package com.yijie.kafka.producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesBuilder {
	protected final static Logger logger = LoggerFactory.getLogger(PropertiesBuilder.class);
//	HelpFormatter formatter = new HelpFormatter();
//	Options options = new Options();
//	
//
//	public PropertiesBuilder() {
//		Option canalConfig = new Option("canalConfig", true, "special config file for canal");
//		Option kafkaConfig = new Option("kafkaConfig", true, "special config file for kafka");
//		Option formatConfig = new Option("formatConfig", true, "special format for kafka message");
//		options.addOption(canalConfig);
//		options.addOption(kafkaConfig);
//		options.addOption(formatConfig);
//	}

	public Properties buildCanalProperties(String[] args) {
		Properties canalProperties = null;
		try {
			InputStream inStream = new FileInputStream(System.getProperty("canalConfig"));
			canalProperties = new Properties();
			canalProperties.load(inStream);
		} catch (IOException e) {
			logger.error("Load canal config failed.  Reason: " + e.getMessage());
			return null;
		}
		return canalProperties;
	}

	public Properties buildKafkaProperties(String[] args) {
		Properties kafkaProperties = null;
		try {
			InputStream inStream = new FileInputStream(System.getProperty("kafkaConfig"));
			kafkaProperties = new Properties();
			kafkaProperties.load(inStream);
		} catch (IOException e) {
			logger.error("Load canal config failed.  Reason: " + e.getMessage());
			return null;
		}
		return kafkaProperties;
	}
	
	public String getFormat(String[] args) {
		String format = System.getProperty("formatConfig");
		if (format == null){
			format = "json";
		}
		
		return format;
	}
}
