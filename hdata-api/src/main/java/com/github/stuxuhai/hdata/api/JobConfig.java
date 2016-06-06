
package com.github.stuxuhai.hdata.api;

public abstract class JobConfig extends Configuration {

	private static final long serialVersionUID = 1L;

	public abstract PluginConfig getReaderConfig();

	public abstract PluginConfig getWriterConfig();

	public abstract String getReaderName();

	public abstract String getWriterName();

	public abstract Reader newReader();

	public abstract Splitter newSplitter();

	public abstract Writer newWriter();
}
