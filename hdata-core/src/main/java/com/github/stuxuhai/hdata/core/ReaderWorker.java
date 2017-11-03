package com.github.stuxuhai.hdata.core;

import java.util.concurrent.Callable;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.RecordCollector;

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
        Thread.currentThread().setContextClassLoader(reader.getClass().getClassLoader());
        reader.prepare(context, readerConfig);
        reader.execute(rc);
        reader.close();
        return 0;
    }

}
