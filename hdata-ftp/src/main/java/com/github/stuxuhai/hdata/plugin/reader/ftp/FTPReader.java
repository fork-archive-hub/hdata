package com.github.stuxuhai.hdata.plugin.reader.ftp;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

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
import com.github.stuxuhai.hdata.ftp.FTPUtils;

public class FTPReader extends Reader {

	private Fields fields;
	private String host;
	private int port;
	private String username;
	private String password;
	private String fieldsSeparator;
	private String encoding;
	private int fieldsCount;
	private int startRow;
	private List<String> files = new ArrayList<String>();

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		host = readerConfig.getString(FTPReaderProperties.HOST);
		port = readerConfig.getInt(FTPReaderProperties.PORT, 21);
		username = readerConfig.getString(FTPReaderProperties.USERNAME, "anonymous");
		password = readerConfig.getString(FTPReaderProperties.PASSWORD, "");
		fieldsSeparator = StringEscapeUtils
				.unescapeJava(readerConfig.getString(FTPReaderProperties.FIELDS_SEPARATOR, "\t"));
		encoding = readerConfig.getString(FTPReaderProperties.ENCODING, "UTF-8");
		files = (List<String>) readerConfig.get(FTPReaderProperties.FILES);
		fieldsCount = readerConfig.getInt(FTPReaderProperties.FIELDS_COUNT, 0);
		startRow = readerConfig.getInt(FTPReaderProperties.START_ROW, 1);

		if (readerConfig.containsKey(FTPReaderProperties.SCHEMA)) {
			fields = new Fields();
			String[] tokens = readerConfig.getString(FTPReaderProperties.SCHEMA).split("\\s*,\\s*");
			for (String field : tokens) {
				fields.add(field);
			}
		}
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		FTPClient ftpClient = null;
		try {
			ftpClient = FTPUtils.getFtpClient(host, port, username, password);
			for (String file : files) {
				InputStream is = ftpClient.retrieveFileStream(file);
				BufferedReader br = null;
				if (file.endsWith(".gz")) {
					GZIPInputStream gzin = new GZIPInputStream(is);
					br = new BufferedReader(new InputStreamReader(gzin, encoding));
				} else {
					br = new BufferedReader(new InputStreamReader(is, encoding));
				}

				String line = null;
				long currentRow = 0;
				while ((line = br.readLine()) != null) {
					currentRow++;
					if (currentRow >= startRow) {
						String[] tokens = StringUtils.splitPreserveAllTokens(line, fieldsSeparator);
						if (tokens.length >= fieldsCount) {
							Record record = new DefaultRecord(tokens.length);
							for (String field : tokens) {
								record.add(field);
							}
							recordCollector.send(record);
						}
					}
				}
				ftpClient.completePendingCommand();
				br.close();
				is.close();
			}
		} catch (Exception e) {
			throw new HDataException(e);
		} finally {
			FTPUtils.closeFtpClient(ftpClient);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	@Override
	public Splitter newSplitter() {
		return new FTPSplitter();
	}

}
