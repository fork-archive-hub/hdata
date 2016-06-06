package com.github.stuxuhai.hdata.plugin.writer.kafka;

import org.apache.commons.lang3.StringEscapeUtils;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaWriter extends Writer {

	private String topic = null;
	private String separator = null;
	private Producer<String, String> producer;
	private Object[] array = null;

	@Override
	public void prepare(JobContext context, PluginConfig writerConfig) {
		topic = writerConfig.getString(KafkaWriterProperties.TOPIC);
		Preconditions.checkNotNull(topic, "Kafka writer required property: topic");

		separator = StringEscapeUtils
				.unescapeJava(writerConfig.getString(KafkaWriterProperties.FIELDS_SEPARATOR, "\t"));
		producer = new Producer<String, String>(new ProducerConfig(writerConfig));
	}

	@Override
	public void execute(Record record) {
		if (array == null) {
			array = new Object[record.size()];
		}

		for (int i = 0, len = record.size(); i < len; i++) {
			array[i] = record.get(i);
		}

		String message = Joiner.on(separator).join(array);
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message, message);
		producer.send(data);
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}
}
