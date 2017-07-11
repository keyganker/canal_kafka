package com.yijie.kafka.producer;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.commons.cli.ParseException;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

public class CanalClient extends AbstractKafkaCanalClient {

    public CanalClient(String destination) {
        super(destination);
    }

    public static void main(String args[]) throws ParseException {
    	PropertiesBuilder builder = new PropertiesBuilder();
    	Properties canalProps = builder.buildCanalProperties(args);
    	Properties kafkaProps = builder.buildKafkaProperties(args);
    	if (canalProps == null || kafkaProps == null) {
    		logger.error("config error while startup");
    		System.exit(0);
    	}
    	
    	String destination = canalProps.getProperty("destination");
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(
        		canalProps.getProperty("server_ip"), Integer.valueOf(canalProps.getProperty("server_port"))), destination, "", "");
        
        YijieKafkaProducer producer = new YijieKafkaProducer(builder.getFormat(args), kafkaProps);
        final CanalClient client = new CanalClient(destination);
        client.setConnector(connector);
        client.setKafkaProducer(producer);
        client.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("Stop the canal client");
                    client.stop();
                } catch (Throwable e) {
                    logger.error("Something goes wrong when stopping canal client:", e);
                } finally {
                    logger.info("Canal client is down.");
                }
            }

        });
    }

}