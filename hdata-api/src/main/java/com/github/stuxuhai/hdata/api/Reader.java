package com.github.stuxuhai.hdata.api;

public abstract class Reader extends AbstractPlugin {

	public void prepare(JobContext context, PluginConfig readerConfig) {
	}

	public void execute(RecordCollector recordCollector) {
	}

	public void close() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public abstract Splitter newSplitter();
}
