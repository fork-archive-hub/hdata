package com.github.stuxuhai.hdata.plugin.reader.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;

public class ConsoleReader extends Reader {

	private BufferedReader br = null;

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		br = new BufferedReader(new InputStreamReader(System.in));
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		try {
			String line = null;
			while ((line = br.readLine()) != null) {
				Record record = new DefaultRecord(1);
				record.add(line);
				recordCollector.send(record);
			}
			br.close();
		} catch (IOException e) {
			new HDataException(e);
		}
	}

	@Override
	public Splitter newSplitter() {
		return null;
	}

}
