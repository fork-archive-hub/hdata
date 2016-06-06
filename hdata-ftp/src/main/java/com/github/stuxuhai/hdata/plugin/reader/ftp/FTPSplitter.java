package com.github.stuxuhai.hdata.plugin.reader.ftp;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.ftp.FTPUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class FTPSplitter extends Splitter {

	private static final Logger LOGGER = LogManager.getLogger(FTPSplitter.class);

	@Override
	public List<PluginConfig> split(JobConfig jobConfig) {
		List<PluginConfig> list = new ArrayList<PluginConfig>();
		PluginConfig readerConfig = jobConfig.getReaderConfig();
		String host = readerConfig.getString(FTPReaderProperties.HOST);
		Preconditions.checkNotNull(host, "FTP reader required property: host");

		int port = readerConfig.getInt(FTPReaderProperties.PORT, 21);
		String username = readerConfig.getString(FTPReaderProperties.USERNAME, "anonymous");
		String password = readerConfig.getString(FTPReaderProperties.PASSWORD, "");
		String dir = readerConfig.getString(FTPReaderProperties.DIR);
		Preconditions.checkNotNull(dir, "FTP reader required property: dir");

		String filenameRegexp = readerConfig.getString(FTPReaderProperties.FILENAME);
		Preconditions.checkNotNull(filenameRegexp, "FTP reader required property: filename");

		boolean recursive = readerConfig.getBoolean(FTPReaderProperties.RECURSIVE, false);
		int parallelism = readerConfig.getParallelism();

		FTPClient ftpClient = null;
		try {
			ftpClient = FTPUtils.getFtpClient(host, port, username, password);
			List<String> files = new ArrayList<String>();
			FTPUtils.listFile(files, ftpClient, dir, filenameRegexp, recursive);
			if (files.size() > 0) {
				if (parallelism == 1) {
					readerConfig.put(FTPReaderProperties.FILES, files);
					list.add(readerConfig);
				} else {
					double step = (double) files.size() / parallelism;
					for (int i = 0; i < parallelism; i++) {
						List<String> splitedFiles = new ArrayList<String>();
						for (int start = (int) Math.ceil(step * i), end = (int) Math
								.ceil(step * (i + 1)); start < end; start++) {
							splitedFiles.add(files.get(start));
						}
						PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
						pluginConfig.put(FTPReaderProperties.FILES, splitedFiles);
						list.add(pluginConfig);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(Throwables.getStackTraceAsString(e));
		} finally {
			FTPUtils.closeFtpClient(ftpClient);
		}

		return list;
	}

}
