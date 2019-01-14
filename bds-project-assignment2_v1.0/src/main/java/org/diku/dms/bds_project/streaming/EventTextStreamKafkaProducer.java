package org.diku.dms.bds_project.streaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Duration;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class EventTextStreamKafkaProducer {
	private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "EventTextStreamProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                    LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * 
     * @param filename the filename of the file to read
     * @param num the number of messages in each batch
     * @param dur the duration between two batches
     * @return a thread instance
     */
    public static Thread startProducerForTextFile(String filename, int num, long dur) {
    	Runnable runnable = () -> {
    		BufferedReader reader = null;
    		while (!Thread.currentThread().isInterrupted()) {
		    try {
		    	reader = new BufferedReader(new FileReader(filename));
		    	while (!Thread.currentThread().isInterrupted()) {
		    		EventTextStreamKafkaProducer.runTextFileProducer(reader, num);
		    		Thread.sleep(dur);
		    	}
			} catch (InterruptedException e) {
				;
			} catch (ExecutionException e) {
				;
			} catch (IOException e) {
				;
			}
    		}
		};
		Thread t = new Thread(runnable);
		t.start();
		return t;
    }
    
    private static void runTextFileProducer(final BufferedReader reader, final int sendMessageCount) throws InterruptedException, ExecutionException, IOException {
    	final Producer<Long, String> producer = createProducer();      
        try {
            for (int i = 0; i < sendMessageCount; ++i) {
            	String line = reader.readLine();
            	if (line == null) break;
            	long timestamp = System.currentTimeMillis();
                final ProducerRecord<Long, String> record = 
                        new ProducerRecord<>(TOPIC, timestamp, line);
                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
    
    //implemented 
    private static void runTweetFileProducer(final BufferedReader reader, final int sendMessageCount) throws InterruptedException, ExecutionException, IOException, TwitterException {
    	final Producer<Long, String> producer = createProducer();      
        try {
            for (int i = 0; i < sendMessageCount; ++i) {
            	String line = reader.readLine();
            	Status s = TwitterObjectFactory.createStatus(line);
            	String txt = s.getText();
            	if (line == null) break;
            	long timestamp = System.currentTimeMillis();
                final ProducerRecord<Long, String> record = 
                        new ProducerRecord<>(TOPIC, timestamp, txt);
                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
    
    /**
     * 
     * @param filename the filename of the twitter data file to read 
     * @param num the number of messages should be sent in this batch
     * @param dur the duration between two batches
     * @return a thread instance
     * @throws TwitterException 
     */
    public static Thread startProduceForTwitterData(String filename, int num, long dur) throws TwitterException {  	
		//Implemented
    	Runnable runnable = () -> {
    		BufferedReader reader = null;
    		while (!Thread.currentThread().isInterrupted()) {
		    try {
		    	reader = new BufferedReader(new FileReader(filename));
		    	while (!Thread.currentThread().isInterrupted()) {
		    		EventTextStreamKafkaProducer.runTweetFileProducer(reader, num);
		    		Thread.sleep(dur);
		    	}
			} catch (InterruptedException e) {
				;
			} catch (ExecutionException e) {
				;
			} catch (IOException e) {
				;
			} catch (TwitterException e) {
				e.printStackTrace();
			}
    		}
		};
		Thread t = new Thread(runnable);
		t.start();
		return t;
    }
    
    //implemented
    private static void runOutOfOrderFileProducer(final BufferedReader reader, final int sendMessageCount,Duration maxDelay) throws InterruptedException, ExecutionException, IOException, TwitterException {
    	final Producer<Long, String> producer = createProducer();      
        try {
            for (int i = 0; i < sendMessageCount; ++i) {
            	String line = reader.readLine();
            	Status s = TwitterObjectFactory.createStatus(line);
            	String txt = s.getText();
            	if (line == null) break;
            	long timestamp = System.currentTimeMillis();
            	//make change to timestamp
            	long delay  = maxDelay.milliseconds();
            	timestamp += delay*Math.random();
                final ProducerRecord<Long, String> record = 
                        new ProducerRecord<>(TOPIC, timestamp, txt);
                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
    /**
     * 
     * @param filename the filename of file to read
     * @param num the number of messages should be sent in this batch
     * @param dur the duration between two batches
     * @param maxEventDelay the largest delay of event time
     * @return a thread instance
     */
    public static Thread startProduceForOutOfOrderData(String filename, int num, long dur, Duration maxDelay) {
    	//Implemented
    	Runnable runnable = () -> {
    		BufferedReader reader = null;
    		while (!Thread.currentThread().isInterrupted()) {
		    try {
		    	reader = new BufferedReader(new FileReader(filename));
		    	while (!Thread.currentThread().isInterrupted()) {
		    		EventTextStreamKafkaProducer.runOutOfOrderFileProducer(reader, num, maxDelay);
		    		Thread.sleep(dur);
		    	}
			} catch (InterruptedException e) {
				;
			} catch (ExecutionException e) {
				;
			} catch (IOException e) {
				;
			} catch (TwitterException e) {
				e.printStackTrace();
			}
    		}
		};
		Thread t = new Thread(runnable);
		t.start();
		return t;
    }
    
}
