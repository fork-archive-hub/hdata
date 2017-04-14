package com.github.stuxuhai.hdata.plugin.reader.csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import com.github.stuxuhai.hdata.plugin.FormatConf;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;

public class CSVReader extends Reader {

	private String path = null;
	private int startRow = 1;
	private String encoding = null;
	private String format;
	private CSVFormat csvFormat = CSVFormat.DEFAULT;

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		path = readerConfig.getString(CSVReaderProperties.PATH);
		startRow = readerConfig.getInt(CSVReaderProperties.START_ROW, 1);
		encoding = readerConfig.getString(CSVReaderProperties.ENCODING, "UTF-8");
		format = readerConfig.getString(CSVReaderProperties.FORMAT);
		FormatConf.confCsvFormat(format,csvFormat);
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		long currentRow = 0;
		try {
			java.io.Reader in = new InputStreamReader(new FileInputStream(path), encoding);
			Iterable<CSVRecord> records = csvFormat.parse(in);
			for (CSVRecord csvRecord : records) {
				currentRow++;
				if (currentRow >= startRow) {
					Record hdataRecord = new DefaultRecord(csvRecord.size());
					for (int i = 0, len = csvRecord.size(); i < len; i++) {
						hdataRecord.add(csvRecord.get(i));
					}
					recordCollector.send(hdataRecord);
				}
			}
		} catch (UnsupportedEncodingException e) {
			throw new HDataException(e);
		} catch (IOException e) {
			throw new HDataException(e);
		}
	}

	@Override
	public Splitter newSplitter() {
		return new CSVSplitter();
	}

}
