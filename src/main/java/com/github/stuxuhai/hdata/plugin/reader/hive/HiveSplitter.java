/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.hive;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.config.JobConfig;
import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Splitter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class HiveSplitter extends Splitter {

    private static final Logger LOG = LogManager.getLogger(HiveSplitter.class);

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String metastoreUris = readerConfig.getString(HiveReaderProperties.METASTORE_URIS);
        Preconditions.checkNotNull(metastoreUris, "Hive reader required property: metastore.uris");

        String hadoopUser = readerConfig.getString(HiveReaderProperties.HADOOP_USER);
        if (hadoopUser != null) {
            System.setProperty("HADOOP_USER_NAME", hadoopUser);
        }

        String dbName = readerConfig.getString(HiveReaderProperties.DATABASE, "default");
        String tableName = readerConfig.getString(HiveReaderProperties.TABLE);
        Preconditions.checkNotNull(tableName, "Hive reader required property: table");

        int parallelism = readerConfig.getParallelism();

        ReadEntity.Builder builder = new ReadEntity.Builder();
        ReadEntity entity = null;
        if (readerConfig.containsKey(HiveReaderProperties.PARTITIONS)) {
            entity = builder.withDatabase(dbName).withTable(tableName).withFilter(readerConfig.getString(HiveReaderProperties.PARTITIONS)).build();
        } else {
            entity = builder.withDatabase(dbName).withTable(tableName).build();
        }

        Map<String, String> config = new HashMap<String, String>();
        config.put(ConfVars.METASTOREURIS.varname, metastoreUris);

        Configuration conf = new Configuration();
        if (readerConfig.containsKey(HiveReaderProperties.HDFS_CONF_PATH)) {
            conf.addResource(new Path("file://" + readerConfig.getString(HiveReaderProperties.HDFS_CONF_PATH)));
        }

        Iterator<Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            config.put(entry.getKey(), entry.getValue());
        }

        HCatReader masterReader = DataTransferFactory.getHCatReader(entity, config);

        try {
            ReaderContext readerContext = masterReader.prepareRead();
            int numSplits = readerContext.numSplits();
            readerConfig.put(HiveReaderProperties.READER_CONTEXT, readerContext);

            Method getConfMethod = readerContext.getClass().getDeclaredMethod("getConf");
            getConfMethod.setAccessible(true);
            HCatSchema schema = HCatBaseInputFormat.getTableSchema((Configuration) getConfMethod.invoke(readerContext));
            readerConfig.put(HiveReaderProperties.FIELDS, schema.getFieldNames());

            Map<String, String> fieldSchemaMap = Maps.newHashMap();
            List<HCatFieldSchema> fields = schema.getFields();
            for (HCatFieldSchema field : fields) {
                fieldSchemaMap.put(field.getName(), field.getTypeString());
            }
            readerConfig.put(HiveReaderProperties.COLUMNS_TYPES, fieldSchemaMap);

            if (parallelism > numSplits) {
                parallelism = numSplits;
                LOG.info("Reader parallelism is greater than input splits count, so parallelism is set to equal with input splits count.");
            }

            if (parallelism == 1) {
                List<Integer> splitList = new ArrayList<Integer>();
                for (int i = 0; i < numSplits; i++) {
                    splitList.add(i);
                }
                readerConfig.put(HiveReaderProperties.INPUT_SPLITS, splitList);
                list.add(readerConfig);
            } else {
                double step = (double) numSplits / parallelism;
                for (int i = 0; i < parallelism; i++) {
                    List<Integer> splitList = new ArrayList<Integer>();
                    for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                        splitList.add(start);
                    }
                    PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                    pluginConfig.put(HiveReaderProperties.INPUT_SPLITS, splitList);
                    list.add(pluginConfig);
                }
            }

            return list;
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }
}
