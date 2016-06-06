package com.github.stuxuhai.hdata.plugin.reader.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class KafkaConsumer implements Runnable {

	private final PluginConfig readerConfig;
	private final RecordCollector recordCollector;
	private boolean stop;
	private ConsumerConnector connector;

	public KafkaConsumer(PluginConfig readerConfig, RecordCollector recordCollector) {
		this.readerConfig = readerConfig;
		this.recordCollector = recordCollector;
	}

	private List<Broker> getBrokers(String zkServers, int sessionTimeout, int connectionTimeout) {
		List<Broker> brokers = new ArrayList<Broker>();
		ZkClient zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout,
				new BytesPushThroughSerializer());
		String zkPath = "/brokers/ids";
		List<String> ids = zkClient.getChildren(zkPath);
		for (String id : ids) {
			byte[] zkData = zkClient.readData(zkPath + "/" + id);
			Gson gson = new Gson();
			Map<String, String> map = gson.fromJson(new String(zkData), new TypeToken<Map<String, String>>() {
				private static final long serialVersionUID = 1L;
			}.getType());
			Broker broker = new Broker(Integer.valueOf(id), map.get("host"), Integer.valueOf(map.get("port")));
			brokers.add(broker);
		}
		zkClient.close();
		return brokers;
	}

	@Override
	public void run() {
		String topic = readerConfig.getProperty(KafkaReaderProperties.TOPIC);
		Preconditions.checkNotNull(topic, "Kafka reader required property: topic");

		String groupID = readerConfig.getProperty(KafkaReaderProperties.GROUP_ID);
		Preconditions.checkNotNull(groupID, "Kafka reader required property: group.id");

		int streamCount = readerConfig.getInt(KafkaReaderProperties.CONSUME_STREAM_COUNT, 1);
		String encoding = readerConfig.getString(KafkaReaderProperties.ENCODING, "UTF-8");
		int maxMessageCount = readerConfig.getInt(KafkaReaderProperties.MAX_FETCH_SIZE, 100000);
		String separator = null;
		if (readerConfig.containsKey(KafkaReaderProperties.FIELDS_SEPARATOR)) {
			separator = StringEscapeUtils.unescapeJava(readerConfig.getString(KafkaReaderProperties.FIELDS_SEPARATOR));
		}

		int receivedMessageCount = 0;
		if (readerConfig.containsKey(KafkaReaderProperties.START_OFFSET)) {
			List<Broker> brokers = getBrokers(readerConfig.getString(KafkaReaderProperties.ZOOKEEPER_CONNECT), 30000,
					30000);

			int partition = readerConfig.getInt(KafkaReaderProperties.PARTITION_ID, 0);
			long offset = readerConfig.getLong(KafkaReaderProperties.START_OFFSET, 0);
			int maxFetchSize = readerConfig.getInt(KafkaReaderProperties.MAX_FETCH_SIZE, 10000);
			for (Broker broker : brokers) {
				SimpleConsumer consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, groupID);

				loop: while (!stop) {
					FetchRequest req = new FetchRequestBuilder().clientId(groupID)
							.addFetch(topic, partition, offset, 1024 * 1024).build();
					FetchResponse fetchResponse = consumer.fetch(req);
					ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partition);
					for (MessageAndOffset messageAndOffset : messageSet) {
						receivedMessageCount++;
						if (receivedMessageCount > maxFetchSize) {
							break loop;
						}

						if (receivedMessageCount != 1 && offset == messageAndOffset.offset()) {
							break loop;
						}

						ByteBuffer payload = messageAndOffset.message().payload();
						byte[] bytes = new byte[payload.limit()];
						payload.get(bytes);

						try {
							String message = new String(bytes, encoding);
							Record record = null;
							if (separator == null) {
								record = new DefaultRecord(1);
								record.add(message);
							} else {
								String[] tokens = StringUtils.splitPreserveAllTokens(message, separator);
								record = new DefaultRecord(tokens.length);
								for (String field : tokens) {
									record.add(field);
								}
							}

							recordCollector.send(record);
							if (receivedMessageCount >= maxMessageCount) {
								break loop;
							}
						} catch (UnsupportedEncodingException e) {
							throw new HDataException(e);
						}

						offset = messageAndOffset.offset();
					}
				}

				consumer.close();
			}
		} else {
			ConsumerConfig config = new ConsumerConfig(readerConfig);
			connector = Consumer.createJavaConsumerConnector(config);
			Map<String, Integer> topics = new HashMap<String, Integer>();
			topics.put(topic, streamCount);

			Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
			List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);

			for (KafkaStream<byte[], byte[]> partition : partitions) {
				ConsumerIterator<byte[], byte[]> it = partition.iterator();
				while (!stop && it.hasNext()) {
					try {
						receivedMessageCount++;
						String message = new String(it.next().message(), encoding);
						Record record = null;
						if (separator == null) {
							record = new DefaultRecord(1);
							record.add(message);
						} else {
							String[] tokens = StringUtils.splitPreserveAllTokens(message, separator);
							record = new DefaultRecord(tokens.length);
							for (String field : tokens) {
								record.add(field);
							}
						}

						recordCollector.send(record);
						if (receivedMessageCount >= maxMessageCount) {
							stop = true;
							break;
						}
					} catch (UnsupportedEncodingException e) {
						throw new HDataException(e);
					}
				}
			}

			connector.shutdown();
		}
	}

	public void stop() {
		stop = true;
		if (connector != null) {
			connector.shutdown();
		}
	}
}
