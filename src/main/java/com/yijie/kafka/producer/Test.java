package com.yijie.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by fey6779 on 2016/10/24.
 */
public class Test {
    public static void main(String[] args) throws IOException {
//        Properties props = new Properties();
//
//        props.put("bootstrap.servers", "10.21.200.48:9092");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//        for(int i = 0; i < 10; i++)
//            producer.send(new ProducerRecord<>("stephen", Integer.toString(i), Integer.toString(i)));
//
//        producer.close();
//    	PropertiesBuilder builder = new PropertiesBuilder();
//    	builder.buildCanalProperties(args);
    	
//    	Schema schema = new Schema.Parser().parse(new File(Test.class.getClassLoader().getResource("user.avsc").getPath()));
//    	GenericRecord user1 = new GenericData.Record(schema);
//    	user1.put("name", "Alyssa");
//    	user1.put("favorite_number", 256);
//    	// Leave favorite color null
//
//    	GenericRecord user2 = new GenericData.Record(schema);
//    	user2.put("name", "Ben");
//    	user2.put("favorite_number", 7);
//    	user2.put("favorite_color", "red");

//    	File file = new File(Test.class.getClassLoader().getResource("user.avro").getPath());
//    	ByteArrayOutputStream file = new ByteArrayOutputStream();
//    	DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
//    	DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
//    	dataFileWriter.create(schema, file);
//    	dataFileWriter.append(user1);
//    	dataFileWriter.append(user2);
//    	dataFileWriter.close();
//    	
//    	SeekableByteArrayInput ss = new SeekableByteArrayInput(file.toByteArray());
//    	DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
//    	DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(ss, datumReader);
//    	GenericRecord user = null;
//    	while (dataFileReader.hasNext()) {
//	    	// Reuse user object by passing it to next(). This saves us from
//	    	// allocating and garbage collecting many objects for files with
//	    	// many items.
//	    	user = dataFileReader.next(user);
//	    	System.out.println(user);
//    	}
    	  Logger log = LoggerFactory.getLogger(Test.class);
//    	  log.trace("======trace");  
//          log.debug("======debug {} {}", "aa", "bb");  
//          log.info("======info {} {}", "aa", "bb");  
//          log.warn("======warn");  
//          log.error("======error"); 
//    	  Exception e = new Exception("sss");
//    	  log.error("great", e);
//        try {
//            int size = 100000;
//            Class.forName("com.mysql.jdbc.Driver");
//            Connection conn = DriverManager.getConnection("jdbc:mysql://10.4.54.101:3306/canal_test", "root", "abc123_");
//            conn.setAutoCommit(false);
//            String sql = "INSERT ftable(id,name,mobile) VALUES(?,?,?)";
//            PreparedStatement prest = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
//                    ResultSet.CONCUR_READ_ONLY);
//            for (int x = 0; x < size; x++) {
//                prest.setInt(1, x+1);
//                prest.setString(2, "name"+x);
//                prest.setInt(3, x);
//                prest.addBatch();
//                if (x%1000==0){
//                    prest.executeBatch();
//                    conn.commit();
//                    prest.clearBatch();
//                }
//            }
//
//            conn.close();
//        } catch (SQLException ex) {
//            log.info(ex.getMessage());
//        } catch (ClassNotFoundException ex) {
//            log.info(ex.getMessage());
//        }

        Yaml yaml = new Yaml();
        Map<String, Map<String, Object>> me = (Map<String, Map<String, Object>>) yaml.load(new FileInputStream(new File(Test.class.getClassLoader().getResource("blacklist.yaml").getPath())));
        Map<String, Object> tables = me.get("fund");
        List<String> fields = (List<String>) tables.get("fund_account");
        if (fields.contains("cardNo")) {
            System.out.println("Yes");
        }

    }
}
