package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Writer;
import com.lmax.disruptor.WorkHandler;

public class RecordWorkHandler implements WorkHandler<RecordEvent> {

    private final Writer writer;
    private final JobContext context;
    private final PluginConfig writerConfig;
    private boolean writerPrepared;
    private final Metric metric;

    public RecordWorkHandler(Writer writer, JobContext context, PluginConfig writerConfig) {
        this.writer = writer;
        this.context = context;
        this.writerConfig = writerConfig;
        this.metric = context.getMetric();
    }

    @Override
    public void onEvent(RecordEvent event) {
        if (!writerPrepared) {
            context.declareOutputFields();
            Thread.currentThread().setContextClassLoader(writer.getClass().getClassLoader());
            writer.prepare(context, writerConfig);
            writerPrepared = true;
            if (metric.getWriterStartTime() == 0) {
                metric.setWriterStartTime(System.currentTimeMillis());
            }
        }

        writer.execute(event.getRecord());
        metric.getWriteCount().incrementAndGet();
    }
}
