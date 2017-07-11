package com.yijie.kafka.producer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.util.Properties;

public class ClusterCanalClient extends AbstractKafkaCanalClient {

    public ClusterCanalClient(String destination){
        super(destination);
    }

    public static void main(String args[]) {
    	PropertiesBuilder builder = new PropertiesBuilder();
    	Properties canalProps = builder.buildCanalProperties(args);
    	Properties kafkaProps = builder.buildKafkaProperties(args);
    	if (canalProps == null || kafkaProps == null) {
    		logger.error("config error while startup");
    		System.exit(0);
    	}

    	String destination = canalProps.getProperty("destination");
    	String canalClusterAddress = canalProps.getProperty("zkServer_addr");
        CanalConnector connector = CanalConnectors.newClusterConnector(canalClusterAddress, destination, "", "");

        YijieKafkaProducer producer = new YijieKafkaProducer(builder.getFormat(args), kafkaProps);
        final ClusterCanalClient canalClient = new ClusterCanalClient(destination);
        canalClient.setConnector(connector);
        canalClient.setKafkaProducer(producer);
        canalClient.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("Stop the cluster canal client");
                    canalClient.stop();
                } catch (Throwable e) {
                    logger.error("Something goes wrong when stopping cluster canal client:", e);
                } finally {
                    logger.info("Cluster canal client is down.");
                }
            }

        });
    }
}