package com.github.stuxuhai.hdata.plugin.reader.csv;

import java.util.ArrayList;
import java.util.List;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.google.common.base.Preconditions;

public class CSVSplitter extends Splitter {

	@Override
	public List<PluginConfig> split(JobConfig jobConfig) {
		List<PluginConfig> list = new ArrayList<PluginConfig>();
		PluginConfig readerConfig = jobConfig.getReaderConfig();

		String paths = readerConfig.getString(CSVReaderProperties.PATH);
		Preconditions.checkNotNull(paths, "CSV reader required property: path");

		if (paths != null) {
			String[] pathArray = paths.split(",");
			for (String path : pathArray) {
				if (!path.trim().isEmpty()) {
					PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
					pluginConfig.put(CSVReaderProperties.PATH, path);
					list.add(pluginConfig);
				}
			}
		}

		return list;
	}
}
