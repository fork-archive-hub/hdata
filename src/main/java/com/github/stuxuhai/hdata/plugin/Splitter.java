/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin;

import java.util.List;

import com.github.stuxuhai.hdata.config.JobConfig;
import com.github.stuxuhai.hdata.config.PluginConfig;

public abstract class Splitter extends AbstractPlugin {

    public abstract List<PluginConfig> split(JobConfig jobConfig);
}
