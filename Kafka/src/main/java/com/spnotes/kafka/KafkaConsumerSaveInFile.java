package com.spnotes.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;

import java.nio.ByteBuffer;
import java.util.*;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.io.UnsupportedEncodingException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.text.Format;
import java.text.SimpleDateFormat;

/*
 * Original code by : Sunil Patil, https://github.com/pppsunil/HelloKafka/blob/master/src/main/java/com/spnotes/kafka/HelloKafkaConsumer.java
 * Modified by : Marianne Linhares Monteiro https://github.com/mari-linhares , 30 jun 2015
 */
public class KafkaConsumerSetNialm extends  Thread {

    final static String clientId = "SimpleConsumerDemoClient";
    final static String TOPIC = "<topic name>";
    final static String ZOOKEEPER_IP ="<zookeeper ip>";   
    final static String GROUP_NAME = "group";

    ConsumerConnector consumerConnector;

    public static void main(String[] argv) throws UnsupportedEncodingException {
        KafkaConsumerSetNialm kafkaConsumer = new KafkaConsumerSetNialm();
       	kafkaConsumer.start();
    }

    public KafkaConsumerSetNialm(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",ZOOKEEPER_IP + ":<zookeeper gate>");
        properties.put("group.id",GROUP_NAME);
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()){
            saveMessageInAFile(new String(it.next().message()));
	}
    }

    private static void saveMessageInAFile(String content){
	try {
	    
		Calendar cal = new GregorianCalendar();
	    	String todayDate = new SimpleDateFormat("dd-MM-yy").format(cal.getTime());
	    	
		//Message will be saved at: <today date>_<topic name>
		File file = new File(todayDate + "_set-nialm");

	 	// if file doesnt exists, then create it
	    	if (!file.exists()) file.createNewFile();
	    	
	    	FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
	    	BufferedWriter bw = new BufferedWriter(fw);
	    	bw.write(content + "\n");
	    	bw.close();
 	   	//System.out.println(content + " Done");
           
	 } catch(IOException exception) { System.out.println("ERROR, message not saved"); }

    }

}
