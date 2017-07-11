package com.yijie.kafka.producer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AbstractKafkaCanalClient {

	protected final static Logger logger = LoggerFactory.getLogger(AbstractKafkaCanalClient.class);
	protected volatile boolean running = false;
	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
		public void uncaughtException(Thread t, Throwable e) {
			logger.error("Uncaught exception occurs: ", e);
		}
	};
	protected Thread thread = null;
	protected CanalConnector connector;
	protected String destination;
	protected YijieKafkaProducer kafkaProducer = null;

	public AbstractKafkaCanalClient(String destination) {
		this(destination, null);
	}

	public AbstractKafkaCanalClient(String destination, CanalConnector connector) {
		this(destination, connector, null);
	}

	public AbstractKafkaCanalClient(String destination, CanalConnector connector, YijieKafkaProducer kafkaProducer) {
		this.connector = connector;
		this.destination = destination;
		this.kafkaProducer = kafkaProducer;
	}

	protected void start() {
		Assert.notNull(connector, "connector is null");
		Assert.notNull(kafkaProducer, "Kafka producer is null");
		thread = new Thread(new Runnable() {
			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
		logger.info("canal client start success...");
	}

	protected void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				logger.error("InterruptedException while stop:", e);
			}
		}

		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
		if (connector != null) {
			connector.disconnect();
		}
		MDC.remove("destination");
	}

	protected void process() {
		int batchSize = 1024;
		long timeout = 200;
		TimeUnit timeUnit = TimeUnit.MILLISECONDS;//毫秒超时
		while (running) {
			try {
				MDC.put("destination", destination);
				connector.connect();
				connector.subscribe();
				while (running) {
					Message message = connector.getWithoutAck(batchSize, timeout, timeUnit);
					long batchId = message.getId();
					try {
						List<Entry> entrys = message.getEntries();
						int size = entrys.size();
						if (batchId == -1 || size == 0) {
							try {
								Thread.sleep(200);//如果没有消息，则隔200毫秒重新获取
							} catch (InterruptedException e) {
							}
						} else {
							for (Entry entry : entrys) {
								long logFileOffset = entry.getHeader().getLogfileOffset();
								String logFileName = entry.getHeader().getLogfileName();
								String dbName = entry.getHeader().getSchemaName();
								String tableName = entry.getHeader().getTableName();
								long timestamp = entry.getHeader().getExecuteTime();
								if (entry.getEntryType() == EntryType.ROWDATA) {
									logger.info("msg[Send message begin] batchId[{}] logFileName[{}] logFileOffset[{}] timestamp[{}] dbName[{}] tableName[{}]",
											batchId, logFileName, logFileOffset, timestamp, dbName, tableName);
									this.sendMessage(entry);
									logger.info("msg[Send message End] batchId[{}] logFileName[{}] logFileOffset[{}] timestamp[{}] dbName[{}] tableName[{}]",
											batchId, logFileName, logFileOffset, timestamp, dbName, tableName);
								}

							}
						}
						
						connector.ack(batchId);
					} catch (Exception e) {
						connector.rollback(batchId);
						logger.error("process canal batch fail, batchId: " + batchId, e);
					}
				}
			} catch (Exception e) {
				logger.error("process error while process:", e);
				if (StringUtils.contains(e.getMessage(), "no alive canal server")) {
					try {
						Thread.sleep(1000);//如果server连不上，则每隔1秒重连一次
					} catch (InterruptedException e1) {
					}
				}
			} finally {
				connector.disconnect();
				MDC.remove("destination");
			}
		}
	}

	/**
	 * { "op": "insert", "dbname": "test", "tablename": "user", "data": { "id":
	 * 1, "name": "dfsaf" }, "beforeChange": {}, "lastModTime": "123123", "ip":
	 * "10.21.100.1" }
	 * 
	 * @param entry
	 * @return
	 */
	protected void sendMessage(Entry entry) {
		this.kafkaProducer.send(entry);
	}
	

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}

	public void setKafkaProducer(YijieKafkaProducer kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
	}
}