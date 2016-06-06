package com.github.stuxuhai.hdata.plugin.reader.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;

public class HttpReader extends Reader {

	private String urlstr = null;
	private String encoding = null;
	private static final Logger LOG = LogManager.getLogger(HttpReader.class);

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		urlstr = readerConfig.getString(HttpReaderProperties.URL);
		encoding = readerConfig.getString(HttpReaderProperties.ENCODING, "UTF-8");
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		URL url;
		try {
			url = new URL(urlstr);
			URLConnection connection = url.openConnection();
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), encoding));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("offset:")) {
					LOG.info(line);
				} else {
					Record record = new DefaultRecord(1);
					record.add(line);
					recordCollector.send(record);
				}
			}
			br.close();
		} catch (MalformedURLException e) {
			throw new HDataException(e);
		} catch (IOException e) {
			throw new HDataException(e);
		}
	}

	@Override
	public Splitter newSplitter() {
		return new HttpSplitter();
	}

}
