package com.github.stuxuhai.hdata.plugin.reader.mongodb;

import java.net.UnknownHostException;
import java.util.Set;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBReader extends Reader {

	private Fields fields;
	private String uri;
	private BasicDBObject condition;
	private static final String OBJECT_ID_KEY = "_id";

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		uri = readerConfig.getString(MongoDBReaderProperties.URI);
		condition = (BasicDBObject) readerConfig.get(MongoDBReaderProperties.QUERY);
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		MongoClientURI clientURI = new MongoClientURI(uri);
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient(clientURI);
			DB db = mongoClient.getDB(clientURI.getDatabase());
			DBCollection coll = db.getCollection(clientURI.getCollection());
			DBCursor cur = coll.find(condition);
			while (cur.hasNext()) {
				DBObject doc = cur.next();
				Set<String> keys = doc.keySet();
				Record record = new DefaultRecord(keys.size() - 1);
				if (fields == null) {
					fields = new Fields();
					for (String key : keys) {
						fields.add(key);
					}
				}

				for (String key : keys) {
					if (!OBJECT_ID_KEY.equals(key)) {
						record.add(doc.get(key));
					}
				}

				recordCollector.send(record);
			}
		} catch (UnknownHostException e) {
			throw new HDataException(e);
		} finally {
			if (mongoClient != null) {
				mongoClient.close();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	@Override
	public Splitter newSplitter() {
		return new MongoDBSplitter();
	}

}
