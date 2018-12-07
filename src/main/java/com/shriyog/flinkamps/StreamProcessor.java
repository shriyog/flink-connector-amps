package com.shriyog.flinkamps;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Shriyog Ingale 07-Dec-2018
 */
public class StreamProcessor {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> ampsStream = env
				.addSource(new AMPSSource("flink-consumer", "tcp://127.0.0.1:9007/amps/json", "test-topic"));

		ampsStream.print();
		env.execute();
	}
}
