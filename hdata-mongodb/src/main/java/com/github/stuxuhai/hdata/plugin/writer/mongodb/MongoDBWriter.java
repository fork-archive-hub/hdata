package com.github.stuxuhai.hdata.plugin.writer.mongodb;

import java.net.UnknownHostException;

import org.apache.commons.lang3.ArrayUtils;

import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBWriter extends Writer {

	private Fields fields;
	private MongoClient mongoClient = null;
	private DBCollection coll;
	private BasicDBObject[] insertDocs;
	private int batchsize;
	private int count;

	@Override
	public void prepare(JobContext context, PluginConfig writerConfig) {
		fields = context.getFields();
		batchsize = writerConfig.getInt(MongoDBWriterProperties.BATCH_INSERT_SIZE, 1000);
		insertDocs = new BasicDBObject[batchsize];

		Preconditions.checkNotNull(writerConfig.getString(MongoDBWriterProperties.URI),
				"MongoDB writer required property: uri");
		MongoClientURI clientURI = new MongoClientURI(writerConfig.getString(MongoDBWriterProperties.URI));
		try {
			mongoClient = new MongoClient(clientURI);
			DB db = mongoClient.getDB(clientURI.getDatabase());
			coll = db.getCollection(clientURI.getCollection());
		} catch (UnknownHostException e) {
			throw new HDataException(e);
		}
	}

	@Override
	public void execute(Record record) {
		BasicDBObject doc = new BasicDBObject();
		for (int i = 0, len = fields.size(); i < len; i++) {
			doc.put(fields.get(i), record.get(i));
		}

		insertDocs[count++] = doc;
		if (count == batchsize) {
			coll.insert(insertDocs);
			count = 0;
		}
	}

	@Override
	public void close() {
		if (mongoClient != null) {
			if (count > 0) {
				coll.insert(ArrayUtils.subarray(insertDocs, 0, count));
			}
			mongoClient.close();
		}
	}
}
