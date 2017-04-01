package com.github.stuxuhai.hdata.plugin.reader.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class HDFSSplitter extends Splitter {

	private static final Logger LOGGER = LogManager.getLogger(HDFSSplitter.class);

	@Override
	public List<PluginConfig> split(JobConfig jobConfig) {
		List<PluginConfig> list = new ArrayList<PluginConfig>();
		List<Path> matchedFiles = new ArrayList<Path>();
		PluginConfig readerConfig = jobConfig.getReaderConfig();
		String hadoopUser = readerConfig.getString(HDFSReaderProperties.HADOOP_USER);
		if (hadoopUser != null) {
			System.setProperty("HADOOP_USER_NAME", hadoopUser);
		}

		Path dir = new Path(readerConfig.getString(HDFSReaderProperties.DIR));
		Preconditions.checkNotNull(dir, "HDFS reader required property: dir");

		int parallelism = readerConfig.getParallelism();

		String hadoopUserName = readerConfig.getString(HDFSReaderProperties.HADOOP_USER);
		if (hadoopUserName != null) {
			System.setProperty("HADOOP_USER_NAME", hadoopUserName);
		}

		Configuration conf = new Configuration();
		if (readerConfig.containsKey(HDFSReaderProperties.HDFS_CONF_PATH)) {
			for (String path: readerConfig.getString(HDFSReaderProperties.HDFS_CONF_PATH).split(",")) {
				conf.addResource(new Path("file://" + path));
			}
		}
		try {
			FileSystem fs = dir.getFileSystem(conf);
			Preconditions.checkNotNull(readerConfig.getString(HDFSReaderProperties.FILENAME_REGEXP),
					"HDFS reader required property: filename");
			Pattern filenamePattern = Pattern.compile(readerConfig.getString(HDFSReaderProperties.FILENAME_REGEXP));
			if (fs.exists(dir)) {
				for (FileStatus fileStatus : fs.listStatus(dir)) {
					Matcher m = filenamePattern.matcher(fileStatus.getPath().getName());
					if (m.matches()) {
						matchedFiles.add(fileStatus.getPath());
					}
				}

				if (matchedFiles.size() > 0) {
					if (parallelism == 1) {
						readerConfig.put(HDFSReaderProperties.FILES, matchedFiles);
						list.add(readerConfig);
					} else {
						double step = (double) matchedFiles.size() / parallelism;
						for (int i = 0; i < parallelism; i++) {
							List<Path> splitedFiles = new ArrayList<Path>();
							for (int start = (int) Math.ceil(step * i), end = (int) Math
									.ceil(step * (i + 1)); start < end; start++) {
								splitedFiles.add(matchedFiles.get(start));
							}
							PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
							pluginConfig.put(HDFSReaderProperties.FILES, splitedFiles);
							list.add(pluginConfig);
						}
					}
				}

			} else {
				LOGGER.error(String.format("Path %s not found.", dir));
			}
		} catch (IOException e) {
			LOGGER.error(Throwables.getStackTraceAsString(e));
		}

		return list;
	}
}
