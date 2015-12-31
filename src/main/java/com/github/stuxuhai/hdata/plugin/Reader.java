/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.core.OutputFieldsDeclarer;

public abstract class Reader extends AbstractPlugin {

    public void prepare(JobContext context, PluginConfig readerConfig) {
    }

    public void execute(RecordCollector recordCollector) {
    }

    public void close() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public abstract Class<? extends Splitter> getSplitterClass();
}
