package com.github.stuxuhai.hdata.plugin.reader.hive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

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
import com.github.stuxuhai.hdata.plugin.hive.HiveTypeUtils;

public class HiveReader extends Reader {

	private final Fields fields = new Fields();
	private List<Integer> splitList = null;
	private ReaderContext readerContext = null;
	private Map<String, String> fieldSchemaMap = null;
	private boolean convertNull;
	private List<Integer> selectColumnsIndexList = new ArrayList<>();

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

		String selectColumnsStr = readerConfig.getString(HiveReaderProperties.SELECT_COLUMNS);
		if (selectColumnsStr == null) {
			for (int i = 0; i < fields.size(); i++) {
				selectColumnsIndexList.add(i);
			}
		} else {
			String[] tokens = selectColumnsStr.split(",");
			for (String token : tokens) {
				int colunmnIndex = searchColumnsIndex(token);
				if (colunmnIndex < 0) {
					throw new HDataException("Column " + token + "not found");
				}
				selectColumnsIndexList.add(colunmnIndex);
			}
		}
	}

	private int searchColumnsIndex(String colunm) {
		for (int i = 0; i < fields.size(); i++) {
			if (colunm.equals(fields.get(i))) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		try {
			for (int slaveNumber : splitList) {
				HCatReader slaveReader = DataTransferFactory.getHCatReader(readerContext, slaveNumber);
				Iterator<HCatRecord> itr = slaveReader.read();
				while (itr.hasNext()) {
					HCatRecord hCatRecord = itr.next();
					Record record = new DefaultRecord(selectColumnsIndexList.size());
					for (int i : selectColumnsIndexList) {
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
	public Splitter newSplitter() {
		return new HiveSplitter();
	}

}
