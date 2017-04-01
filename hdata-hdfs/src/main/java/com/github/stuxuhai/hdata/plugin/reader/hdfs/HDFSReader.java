package com.github.stuxuhai.hdata.plugin.reader.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

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

public class HDFSReader extends Reader {

	private Fields fields;
	private String fieldsSeparator;
	private String encoding;
	private PluginConfig readerConfig;
	private List<Path> files = new ArrayList<Path>();

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		this.readerConfig = readerConfig;
		fieldsSeparator = StringEscapeUtils
				.unescapeJava(readerConfig.getString(HDFSReaderProperties.FIELDS_SEPARATOR, "\t"));
		files = (List<Path>) readerConfig.get(HDFSReaderProperties.FILES);
		encoding = readerConfig.getString(HDFSReaderProperties.ENCODING, "UTF-8");

		String hadoopUser = readerConfig.getString(HDFSReaderProperties.HADOOP_USER);
		if (hadoopUser != null) {
			System.setProperty("HADOOP_USER_NAME", hadoopUser);
		}

		if (readerConfig.containsKey(HDFSReaderProperties.SCHEMA)) {
			fields = new Fields();
			String[] tokens = readerConfig.getString(HDFSReaderProperties.SCHEMA).split("\\s*,\\s*");
			for (String field : tokens) {
				fields.add(field);
			}
		}
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		Configuration conf = new Configuration();
		if (readerConfig.containsKey(HDFSReaderProperties.HDFS_CONF_PATH)) {
			for (String path: readerConfig.getString(HDFSReaderProperties.HDFS_CONF_PATH).split(",")) {
				conf.addResource(new Path("file://" + path));
			}
		}

		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		try {
			for (Path file : files) {
				FileSystem fs = file.getFileSystem(conf);
				CompressionCodec codec = codecFactory.getCodec(file);
				FSDataInputStream input = fs.open(file);
				BufferedReader br;
				String line = null;
				if (codec == null) {
					br = new BufferedReader(new InputStreamReader(input, encoding));
				} else {
					br = new BufferedReader(new InputStreamReader(codec.createInputStream(input), encoding));
				}
				while ((line = br.readLine()) != null) {
					String[] tokens = StringUtils.splitPreserveAllTokens(line, fieldsSeparator);
					Record record = new DefaultRecord(tokens.length);
					for (String field : tokens) {
						record.add(field);
					}
					recordCollector.send(record);
				}
				br.close();
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
	public Splitter newSplitter() {
		return new HDFSSplitter();
	}
}
