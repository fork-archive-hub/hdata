/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.JobContext;

public abstract class Writer extends AbstractPlugin {

    public void prepare(JobContext context, PluginConfig writerConfig) {
    }

    public void execute(Record record) {
    }

    public void close() {
    }
}
