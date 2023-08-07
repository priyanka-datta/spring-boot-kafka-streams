package com.priyanka.datta.kafkastreamsprogram;

import com.priyanka.datta.kafkastreamsprogram.service.StreamTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class KafkaStreamsProgramApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsProgramApplication.class, args);

		Properties map = new Properties();
		map.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"stream-app");
		map.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		//map.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		//map.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		//map.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1800");
		Topology topology = StreamTopology.createTopology();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, map);
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
		try {
			kafkaStreams.start();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}


	}

}
