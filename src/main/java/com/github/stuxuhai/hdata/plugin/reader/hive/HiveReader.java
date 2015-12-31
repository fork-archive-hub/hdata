/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.hive;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

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
import com.github.stuxuhai.hdata.util.HiveTypeUtils;

public class HiveReader extends Reader {

    private final Fields fields = new Fields();
    private List<Integer> splitList = null;
    private ReaderContext readerContext = null;
    private Map<String, String> fieldSchemaMap = null;
    private boolean convertNull;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        if (readerConfig.containsKey(HiveReaderProperties.HADOOP_USER)) {
            System.setProperty("HADOOP_USER_NAME", readerConfig.getString(HiveReaderProperties.HADOOP_USER));
        }

        convertNull = readerConfig.getBoolean(HiveReaderProperties.CONVERT_NULL, true);
        splitList = (List<Integer>) readerConfig.get(HiveReaderProperties.INPUT_SPLITS);
        readerContext = (ReaderContext) readerConfig.get(HiveReaderProperties.READER_CONTEXT);
        fieldSchemaMap = (Map<String, String>) readerConfig.get(HiveReaderProperties.COLUMNS_TYPES);

        List<String> fieldsList = (List<String>) readerConfig.get(HiveReaderProperties.FIELDS);
        for (String field : fieldsList) {
            fields.add(field);
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            for (int slaveNumber : splitList) {
                HCatReader slaveReader = DataTransferFactory.getHCatReader(readerContext, slaveNumber);
                Iterator<HCatRecord> itr = slaveReader.read();
                while (itr.hasNext()) {
                    HCatRecord hCatRecord = itr.next();
                    Record record = new DefaultRecord(hCatRecord.size());
                    for (int i = 0, len = hCatRecord.size(); i < len; i++) {
                        String columnsTypes = fieldSchemaMap.get(fields.get(i));
                        Object obj = hCatRecord.get(i);
                        record = HiveTypeUtils.convertHiveSpecialValue(record, obj, columnsTypes, convertNull);
                    }
                    recordCollector.send(record);
                }
            }
        } catch (HCatException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Class<? extends Splitter> getSplitterClass() {
        return HiveSplitter.class;
    }

}
