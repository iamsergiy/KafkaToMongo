package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.UUID;
import org.apache.mongo.*;

/**
 * Consumer of the data, taken from Kafka and storing it into MongoDB
 *
 * Consumidor de los datos de sitios Web de Kafka y su almacenamiento en MongoDB
 * Usage:
 * 		5 args:
 * 			"host (MongoDB)" -> localhost
 * 			"port (MongoDB)" -> 27017
 * 			"name of db to use (MongoDB)" -> 1usagov
 * 			"name of collection to use (MongoDB)" -> clicks
 * 			"name of Kafka's topic" -> 1usagov
 *
 * 	@author sergiy.shvayka
 */
public class Consumer {

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("Usage: " +
					Consumer.class.getName() + " " +
					"'{host}' " +
					"'{port}' " +
					"'{dbName}' " +
					"'{collectionName}' " +
					"'{topic}' ");
			return;
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);
		String name_db = args[2];
		String collName = args[3];
		String topic = args[4];

		UUID id = UUID.randomUUID();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties prop = new Properties();
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("group.id", String.valueOf(id));
		prop.setProperty("auto.offset.reset", "latest");
		prop.setProperty("zookeeper.connect", "localhost:2181");
		prop.setProperty("topic", topic);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>(
				prop.getProperty("topic"),
				new SimpleStringSchema(),
				prop));

		DataStream<Tuple2<String,Integer>> stream2 = messageStream.flatMap(new LineSplitter());

		stream2.keyBy(0).addSink(new WriteToMongo(host, port, name_db, collName));

		env.execute("Flink Streaming Java From Kafka");
	}

	private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\n");

			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}