package com.shriyog.flinkamps;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Message;

/**
 * @author Shriyog Ingale 07-Dec-2018
 */
public class AMPSSource extends RichSourceFunction<String> {

	private static final long serialVersionUID = -8708182052610791593L;
	private String name, topic, connectionString;
	private Client client;

	public AMPSSource(String name, String connectionString, String topic) {
		this.name = name;
		this.topic = topic;
		this.connectionString = connectionString;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// We create a Client, then connect() and logon()
		client = new Client(this.name);
		client.connect(this.connectionString);
		client.logon();
	}

	public void run(SourceContext<String> sourceContext) throws Exception {
		/*
		 * Here, we iterate over messages in the MessageStream returned by
		 * subscribe method
		 */
		for (Message message : client.subscribe(this.topic)) {
			sourceContext.collect(message.getData());
		}
	}

	@Override
	public void close() throws Exception {
		try {
			cancel();
		} finally {
			super.close();
		}
	}

	public void cancel() {
		client.close();
	}

}
