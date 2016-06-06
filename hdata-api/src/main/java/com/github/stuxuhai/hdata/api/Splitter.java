package com.github.stuxuhai.hdata.api;

import java.util.List;

public abstract class Splitter extends AbstractPlugin {

	public abstract List<PluginConfig> split(JobConfig jobConfig);
}
