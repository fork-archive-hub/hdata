/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

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
import com.google.common.base.Preconditions;

public class HBaseReader extends Reader {

    private final Fields fields = new Fields();
    private Table table;
    private byte[] startRowkey;
    private byte[] endRowkey;
    private String[] columns;
    private int rowkeyIndex = -1;
    private static final String ROWKEY = ":rowkey";

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        startRowkey = (byte[]) readerConfig.get(HBaseReaderProperties.START_ROWKWY);
        endRowkey = (byte[]) readerConfig.get(HBaseReaderProperties.END_ROWKWY);

        Preconditions.checkNotNull(readerConfig.getString(HBaseReaderProperties.SCHEMA), "HBase reader required property: schema");
        String[] schema = readerConfig.getString(HBaseReaderProperties.SCHEMA).split(",");
        for (String field : schema) {
            fields.add(field);
        }

        Configuration conf = HBaseConfiguration.create();
        if (readerConfig.containsKey(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT)) {
            conf.set(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT, readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT));
        }
        conf.set("hbase.zookeeper.quorum", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));

        Preconditions.checkNotNull(readerConfig.getString(HBaseReaderProperties.COLUMNS), "HBase reader required property: columns");
        columns = readerConfig.getString(HBaseReaderProperties.COLUMNS).split("\\s*,\\s*");
        for (int i = 0, len = columns.length; i < len; i++) {
            if (ROWKEY.equalsIgnoreCase(columns[i])) {
                rowkeyIndex = i;
                break;
            }
        }

        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(readerConfig.getString(HBaseReaderProperties.TABLE)));
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        Scan scan = new Scan();
        if (startRowkey.length > 0) {
            scan.setStartRow(startRowkey);
        }
        if (endRowkey.length > 0) {
            scan.setStopRow(endRowkey);
        }

        for (int i = 0, len = columns.length; i < len; i++) {
            if (i != rowkeyIndex) {
                String[] column = columns[i].split(":");
                scan.addColumn(Bytes.toBytes(column[0]), Bytes.toBytes(column[1]));
            }
        }

        try {
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                Record record = new DefaultRecord(fields.size());
                for (int i = 0, len = fields.size(); i < len; i++) {
                    if (i == rowkeyIndex) {
                        record.add(Bytes.toString(result.getRow()));
                    } else {
                        String[] column = columns[i].split(":");
                        record.add(Bytes.toString(result.getValue(Bytes.toBytes(column[0]), Bytes.toBytes(column[1]))));
                    }
                }
                recordCollector.send(record);
            }

            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Class<? extends Splitter> getSplitterClass() {
        return HBaseSplitter.class;
    }
}
