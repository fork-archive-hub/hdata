/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.writer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.google.common.base.Preconditions;

public class HBaseWriter extends Writer {

    private Table table;
    private int batchSize;
    private int rowkeyIndex = -1;
    private final List<Put> putList = new ArrayList<Put>();
    private String[] columns;
    private static final String ROWKEY = ":rowkey";

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        Configuration conf = HBaseConfiguration.create();
        if (writerConfig.containsKey(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT)) {
            conf.set(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT, writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT));
        }

        Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_QUORUM),
                "HBase writer required property: zookeeper.quorum");

        conf.set("hbase.zookeeper.quorum", writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort", writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
        batchSize = writerConfig.getInt(HBaseWriterProperties.BATCH_INSERT_SIZE, 10000);

        Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.COLUMNS), "HBase writer required property: zookeeper.columns");
        columns = writerConfig.getString(HBaseWriterProperties.COLUMNS).split(",");
        for (int i = 0, len = columns.length; i < len; i++) {
            if (ROWKEY.equalsIgnoreCase(columns[i])) {
                rowkeyIndex = i;
                break;
            }
        }

        if (rowkeyIndex == -1) {
            throw new HDataException("Can not find :rowkey in columnsMapping of HBase Writer!");
        }

        try {
            Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.TABLE), "HBase writer required property: table");
            Connection conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(writerConfig.getString(HBaseWriterProperties.TABLE)));
        } catch (IOException e) {
            throw new HDataException(e);
        }

    }

    @Override
    public void execute(Record record) {
        Object rowkeyValue = record.get(rowkeyIndex);
        Put put = new Put(Bytes.toBytes(rowkeyValue == null ? "NULL" : rowkeyValue.toString()));
        for (int i = 0, len = record.size(); i < len; i++) {
            if (i != rowkeyIndex) {
                String[] tokens = columns[i].split(":");
                put.addColumn(Bytes.toBytes(tokens[0]), Bytes.toBytes(tokens[1]),
                        record.get(i) == null ? null : Bytes.toBytes(record.get(i).toString()));
            }
        }

        putList.add(put);
        if (putList.size() == batchSize) {
            try {
                table.put(putList);
            } catch (IOException e) {
                throw new HDataException(e);
            }
            putList.clear();
        }
    }

    @Override
    public void close() {
        if (table != null) {
            try {
                if (putList.size() > 0) {
                    table.put(putList);
                }
                table.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
            putList.clear();
        }
    }
}
