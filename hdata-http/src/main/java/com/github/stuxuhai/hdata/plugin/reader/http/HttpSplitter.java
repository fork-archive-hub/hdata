package com.github.stuxuhai.hdata.plugin.reader.http;

import java.util.ArrayList;
import java.util.List;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.google.common.base.Preconditions;

public class HttpSplitter extends Splitter {

	@Override
	public List<PluginConfig> split(JobConfig jobConfig) {
		List<PluginConfig> list = new ArrayList<PluginConfig>();
		PluginConfig readerConfig = jobConfig.getReaderConfig();

		String urls = readerConfig.getString(HttpReaderProperties.URL);
		Preconditions.checkNotNull(urls, "HTTP reader required property: url");

		String[] urlArray = urls.split(",");
		for (String url : urlArray) {
			if (!url.trim().isEmpty()) {
				PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
				pluginConfig.put(HttpReaderProperties.URL, url);
				list.add(pluginConfig);
			}
		}

		List<Long> ids = new ArrayList<Long>();
		readerConfig.put("ids", ids);

		return list;
	}
}
