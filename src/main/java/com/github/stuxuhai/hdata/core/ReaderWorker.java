/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import java.util.concurrent.Callable;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.RecordCollector;

public class ReaderWorker implements Callable<Integer> {

    private final Reader reader;
    private final JobContext context;
    private final RecordCollector rc;
    private final PluginConfig readerConfig;

    public ReaderWorker(Reader reader, JobContext context, PluginConfig readerConfig, RecordCollector rc) {
        this.reader = reader;
        this.context = context;
        this.rc = rc;
        this.readerConfig = readerConfig;
    }

    @Override
    public Integer call() throws Exception {
        reader.prepare(context, readerConfig);
        reader.execute(rc);
        reader.close();
        return 0;
    }

}
