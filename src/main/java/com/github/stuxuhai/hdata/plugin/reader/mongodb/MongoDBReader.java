/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.mongodb;

import java.net.UnknownHostException;
import java.util.Set;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.core.Fields;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.core.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.RecordCollector;
import com.github.stuxuhai.hdata.plugin.Splitter;
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
    public Class<? extends Splitter> getSplitterClass() {
        return MongoDBSplitter.class;
    }
}
