/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.config.JobConfig;
import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.plugin.Splitter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class HBaseSplitter extends Splitter {

    private static final Logger LOGGER = LogManager.getLogger(HBaseSplitter.class);

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        int parallelism = readerConfig.getParallelism();

        String startRowkey = readerConfig.getString(HBaseReaderProperties.START_ROWKWY, "");
        String endRowkey = readerConfig.getString(HBaseReaderProperties.END_ROWKWY, "");
        byte[] startRowkeyBytes = startRowkey.getBytes();
        byte[] endRowkeyBytes = endRowkey.getBytes();

        if (parallelism == 1) {
            readerConfig.put(HBaseReaderProperties.START_ROWKWY, startRowkeyBytes);
            readerConfig.put(HBaseReaderProperties.END_ROWKWY, endRowkeyBytes);
            list.add(readerConfig);
            return list;
        } else {
            Configuration conf = HBaseConfiguration.create();
            if (readerConfig.containsKey(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT)) {
                conf.set(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT, readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_ZNODE_PARENT));
            }
            String zookeeperQuorum = readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_QUORUM);
            Preconditions.checkNotNull(zookeeperQuorum, "HBase reader required property: zookeeper.quorum");

            conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
            conf.set("hbase.zookeeper.property.clientPort", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
            try {
                Connection conn = ConnectionFactory.createConnection(conf);
                TableName tableName = TableName.valueOf(readerConfig.getString(HBaseReaderProperties.TABLE));
                Table table = conn.getTable(tableName);
                RegionLocator regionLocator = conn.getRegionLocator(tableName);

                Preconditions.checkNotNull(table, "HBase reader required property: table");
                Pair<byte[][], byte[][]> startEndKeysPair = regionLocator.getStartEndKeys();
                table.close();
                List<Pair<byte[], byte[]>> selectedPairList = new ArrayList<Pair<byte[], byte[]>>();
                byte[][] startKeys = startEndKeysPair.getFirst();
                byte[][] endKeys = startEndKeysPair.getSecond();

                if (startKeys.length == 1) {
                    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                    pair.setFirst(startRowkeyBytes);
                    pair.setSecond(endRowkeyBytes);
                    selectedPairList.add(pair);
                } else {
                    if (startRowkeyBytes.length == 0 && endRowkeyBytes.length == 0) {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                            pair.setFirst(startKeys[i]);
                            pair.setSecond(endKeys[i]);
                            selectedPairList.add(pair);
                        }
                    } else if (endRowkeyBytes.length == 0) {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
                                pair.setSecond(endKeys[i]);
                                selectedPairList.add(pair);
                            }
                        }
                    } else {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            if (len == 1) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(startRowkeyBytes);
                                pair.setSecond(endRowkeyBytes);
                                selectedPairList.add(pair);
                                break;
                            } else if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0 && Bytes.compareTo(endRowkeyBytes, startKeys[i]) >= 0) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
                                pair.setSecond(Bytes.compareTo(endKeys[i], endRowkeyBytes) <= 0 ? endKeys[i] : endRowkeyBytes);
                                selectedPairList.add(pair);
                            }
                        }
                    }
                }

                if (parallelism > selectedPairList.size()) {
                    LOGGER.info(
                            "parallelism: {} is greater than the region count: {} in the currently open table: {}, so parallelism is set equal to region count.",
                            parallelism, selectedPairList.size(), table.getName().getNameAsString());
                    parallelism = selectedPairList.size();
                }

                double step = (double) selectedPairList.size() / parallelism;
                for (int i = 0; i < parallelism; i++) {
                    List<Pair<byte[], byte[]>> splitedPairs = new ArrayList<Pair<byte[], byte[]>>();
                    for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                        splitedPairs.add(selectedPairList.get(start));
                    }
                    PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                    pluginConfig.put(HBaseReaderProperties.START_ROWKWY, splitedPairs.get(0).getFirst());
                    pluginConfig.put(HBaseReaderProperties.END_ROWKWY, splitedPairs.get(splitedPairs.size() - 1).getSecond());
                    list.add(pluginConfig);
                }
            } catch (IOException e) {
                LOGGER.error(Throwables.getStackTraceAsString(e));
            }

            return list;
        }
    }
}
