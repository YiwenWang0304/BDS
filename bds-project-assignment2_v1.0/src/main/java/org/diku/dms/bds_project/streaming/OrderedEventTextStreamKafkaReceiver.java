package org.diku.dms.bds_project.streaming;

import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Tuple2;

@SuppressWarnings("serial")
public class OrderedEventTextStreamKafkaReceiver extends Receiver<Tuple2<Long, String>> {
	
	private static String TOPIC = "test";
	private static String BOOTSTRAP_SERVERS = "localhost:9092";
  
	private Consumer<Long, String> consumer;
	
	private Duration maxEventDelay;
	
	/**
	 * 
	 * @param maxEventDelay the maximal delay on event time
	 */
	public OrderedEventTextStreamKafkaReceiver(Duration maxEventDelay) {
		super(StorageLevel.MEMORY_ONLY());
		this.maxEventDelay = maxEventDelay;
		}
	
	public static Consumer<Long, String> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "EventSortedReceiver");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(TOPIC));
        return consumer;
	}
  
	public void onStop() {
		consumer.close();
	}
  
	public void onStart() {
		 new Thread(this::receive).start();  
	}
	
	private void receive() {
      	//implementing
      	consumer = createConsumer();
      	int noMessageFound = 0;
  	 	while (!isStopped()) {
  	 		@SuppressWarnings("deprecation")
			ConsumerRecords<Long, String> records = consumer.poll(1000); // fetch messages using consumer api and await at most 1 second
  	 		if (records.count() == 0) {
  	 			noMessageFound ++;
  	 			if (noMessageFound > 5) break; // if nothing fetched 5 times in a row, stop reading
  	 			else continue;
  	 		}
  	 		noMessageFound = 0;
  	 		//to buffer all the data and
  	 		//only send out the data in order based on the value of maxEventDelay
  	 		Long maxDelay = maxEventDelay.milliseconds();
  	 		int capacity = 0; //need to be changed
			StringBuffer buffer = new StringBuffer();
			Iterator<ConsumerRecord<Long, String>> recIter = records.iterator();
			for(int tmp = 0; tmp<records.count();tmp++) {
				String c = recIter.next().value();
				buffer.insert(tmp,c);
			}
  	 		
  	 		/*records.forEach(record->{
  	 			store(new Tuple2<Long, String>(record.key(), record.value())); // for each record, call store() function provided by SparkStreaming api 
  	 		});*/
			
  	 		consumer.commitAsync();
  	  	  }
  	 	consumer.close();
	}
}


