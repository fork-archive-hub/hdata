package com.github.stuxuhai.hdata.plugin.reader.kafka;

import java.lang.Thread.State;

import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;

public class KafkaReader extends Reader {

	private Fields fields;
	private int maxWaitingSeconds;
	private PluginConfig readerConfig;

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		this.readerConfig = readerConfig;
		maxWaitingSeconds = readerConfig.getInt(KafkaReaderProperties.MAX_WAIT_SECOND, 300);

		if (readerConfig.containsKey("schema")) {
			fields = new Fields();
			String[] tokens = readerConfig.getString("schema").split("\\s*,\\s*");
			for (String field : tokens) {
				fields.add(field);
			}
		}
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		KafkaConsumer consumer = new KafkaConsumer(readerConfig, recordCollector);

		Thread t = new Thread(consumer);
		t.start();

		int sleepedSecond = 0;
		try {
			while (!t.getState().equals(State.TERMINATED)) {
				Thread.sleep(1000);

				sleepedSecond++;
				if (sleepedSecond >= maxWaitingSeconds) {
					consumer.stop();
					break;
				}
			}
		} catch (InterruptedException e) {
			throw new HDataException(e);
		}

		if (sleepedSecond < maxWaitingSeconds) {
			consumer.stop();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	@Override
	public Splitter newSplitter() {
		return null;
	}

}
