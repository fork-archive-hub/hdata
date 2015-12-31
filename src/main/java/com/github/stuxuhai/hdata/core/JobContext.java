/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.common.JobStatus;
import com.github.stuxuhai.hdata.config.Configuration;
import com.github.stuxuhai.hdata.config.EngineConfig;
import com.github.stuxuhai.hdata.config.JobConfig;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.Writer;

public class JobContext {

    private EngineConfig engineConfig;
    private JobConfig jobConfig;
    private OutputFieldsDeclarer declarer;
    private Storage storage;
    private Metric metric;
    private Reader[] readers;
    private Writer[] writers;
    private boolean isReaderFinished;
    private boolean isReaderError;
    private boolean isWriterFinished;
    private boolean isWriterError;
    private JobStatus jobStatus = JobStatus.SUCCESS;

    public Fields getFields() {
        return declarer.getFields();
    }

    protected void setFields(Fields fields) {
        declarer.declare(fields);
    }

    public Configuration getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    protected OutputFieldsDeclarer getDeclarer() {
        return declarer;
    }

    protected void setDeclarer(OutputFieldsDeclarer declarer) {
        this.declarer = declarer;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    public boolean isWriterError() {
        return isWriterError;
    }

    public void setWriterError(boolean isWriterError) {
        this.isWriterError = isWriterError;
    }

    public boolean isReaderFinished() {
        return isReaderFinished;
    }

    public void setReaderFinished(boolean isReaderFinished) {
        this.isReaderFinished = isReaderFinished;
    }

    public boolean isReaderError() {
        return isReaderError;
    }

    public void setReaderError(boolean isReaderError) {
        this.isReaderError = isReaderError;
    }

    public boolean isWriterFinished() {
        return isWriterFinished;
    }

    public void setWriterFinished(boolean isWriterFinished) {
        this.isWriterFinished = isWriterFinished;
    }

    public Reader[] getReaders() {
        return readers;
    }

    public void setReaders(Reader[] readers) {
        this.readers = readers;
    }

    public Writer[] getWriters() {
        return writers;
    }

    public void setWriters(Writer[] writers) {
        this.writers = writers;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public void declareOutputFields() {
        for (Reader reader : readers) {
            if (getFields() == null) {
                reader.declareOutputFields(getDeclarer());
            } else {
                break;
            }
        }
    }
}
