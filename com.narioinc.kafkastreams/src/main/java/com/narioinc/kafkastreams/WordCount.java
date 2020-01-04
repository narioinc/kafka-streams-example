package com.narioinc.kafkastreams;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class WordCount {
	
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream("source-topic");
		KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
            }
        }).groupBy(new KeyValueMapper<String, String, String>() {
            public String apply(String key, String value) {
                return value;
            }
         }).count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as("counts-store"));
		//words.to("sink_topic");
		
		counts.toStream().to("sink-topic", Produced.with(Serdes.String(), Serdes.Long()));
		
		
		final Topology topology = builder.build();
		System.out.println(topology.describe());
		
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);
		
		 // And From your main() method or any other method
		Timer timer = new Timer();
		timer.schedule(new CountStoreReader(streams), 0, 5000);
		 
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
		    @Override
		    public void run() {
		        streams.close();
		        latch.countDown();
		    }
		});
		 
		try {
		    streams.start();
		    latch.await();

		} catch (Throwable e) {
		    System.exit(1);
		}
		System.exit(0);
    }
}

class CountStoreReader extends TimerTask {
	KafkaStreams mStreams;
	
	public CountStoreReader(KafkaStreams streams) {
		this.mStreams = streams;
	}
	
    public void run() {
       getCount(mStreams); 
    }
    
    void getCount(KafkaStreams streams){
		// Get the key-value store CountsKeyValueStore
		if(streams.state() == State.RUNNING) {
			ReadOnlyKeyValueStore<Object, Object> keyValueStore =
					streams.store("counts-store", QueryableStoreTypes.keyValueStore());

			// Get value by key
			System.out.println("count for hello:" + keyValueStore.get("hello"));
		}
		/*// Get the values for a range of keys available in this application instance
		KeyValueIterator<String, Long> range = keyValueStore.range("all", "streams");
		while (range.hasNext()) {
		  KeyValue<String, Long> next = range.next();
		  System.out.println("count for " + next.key + ": " + value);
		}

		// Get the values for all of the keys available in this application instance
		KeyValueIterator<String, Long> range = keyValueStore.all();
		while (range.hasNext()) {
		  KeyValue<String, Long> next = range.next();
		  System.out.println("count for " + next.key + ": " + value);
		}*/
	}
}
