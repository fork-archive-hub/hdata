/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.config;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import com.github.stuxuhai.hdata.api.EngineConfig;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Throwables;

public class DefaultEngineConfig extends EngineConfig {

	private static final long serialVersionUID = 1L;

	private DefaultEngineConfig() {
		super();
	}

	public static DefaultEngineConfig create() {
		DefaultEngineConfig conf = new DefaultEngineConfig();
		String path = Utils.getConfigDir() + Constants.HDATA_XML;

		try {
			XMLConfiguration config = new XMLConfiguration(path);
			config.setValidating(true);

			List<HierarchicalConfiguration> properties = config.configurationsAt(".property");
			for (HierarchicalConfiguration hc : properties) {
				String name = hc.getString("name");
				String value = hc.getString("value");
				conf.setProperty(name, value);
			}
		} catch (ConfigurationException e) {
			Throwables.propagate(e);
		}

		return conf;
	}

}
