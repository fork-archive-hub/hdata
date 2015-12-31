/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.common.HDataConfigConstants;
import com.github.stuxuhai.hdata.common.JobStatus;
import com.github.stuxuhai.hdata.config.EngineConfig;
import com.github.stuxuhai.hdata.config.JobConfig;
import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.RecordCollector;
import com.github.stuxuhai.hdata.plugin.Splitter;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.github.stuxuhai.hdata.util.DataSourceUtils;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Throwables;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class HData {

    private int exitCode = 0;
    private static final DecimalFormat decimalFormat = new DecimalFormat("#0.00");
    private static final Logger LOGGER = LogManager.getLogger(HData.class);

    public void start(final JobConfig jobConfig) {
        final PluginConfig readerConfig = jobConfig.getReaderConfig();
        final PluginConfig writerConfig = jobConfig.getWriterConfig();

        String readerName = jobConfig.getReaderName();
        String writerName = jobConfig.getWriterName();
        LOGGER.info("Reader: {}, Writer: {}", readerName, writerName);

        final JobContext context = new JobContext();
        context.setJobConfig(jobConfig);

        final Metric metric = new Metric();
        context.setMetric(metric);

        final OutputFieldsDeclarer outputFieldsDeclarer = new OutputFieldsDeclarer();
        context.setDeclarer(outputFieldsDeclarer);

        final EngineConfig engineConfig = EngineConfig.create();
        context.setEngineConfig(engineConfig);

        long sleepMillis = engineConfig.getLong(HDataConfigConstants.HDATA_SLEEP_MILLIS, Constants.DEFAULT_HDATA_SLEEP_MILLIS);

        List<PluginConfig> readerConfigList;
        Splitter splitter = jobConfig.newSplitter();
        if (splitter != null) {
            LOGGER.info("Executing splitter for reader.");
            readerConfigList = splitter.split(jobConfig);
            if (readerConfigList.isEmpty()) {
                System.exit(JobStatus.SUCCESS.getStatus());
            }
        } else {
            if (readerConfig.getParallelism() > 1) {
                LOGGER.warn("Can not find splitter, reader parallelism is set to 1.");
            }
            readerConfigList = new ArrayList<PluginConfig>();
            readerConfigList.add(readerConfig);
        }

        Reader[] readers = new Reader[readerConfigList.size()];
        for (int i = 0, len = readers.length; i < len; i++) {
            readers[i] = jobConfig.newReader();
        }

        context.setReaders(readers);

        int writerParallelism = writerConfig.getParallelism();
        LOGGER.info("Reader parallelism: {}, Writer parallelism: {}", readers.length, writerParallelism);

        final Writer[] writers = new Writer[writerParallelism];
        final RecordWorkHandler[] handlers = new RecordWorkHandler[writerParallelism];
        for (int i = 0; i < writerParallelism; i++) {
            writers[i] = jobConfig.newWriter();
            handlers[i] = new RecordWorkHandler(writers[i], context, writerConfig);
        }
        context.setWriters(writers);

        int bufferSize = engineConfig.getInt(HDataConfigConstants.STORAGE_BUFFER_SIZE, 1024);
        String WaitStrategyName = engineConfig.getString(HDataConfigConstants.HDATA_STORAGE_DISRUPTOR_WAIT_STRATEGY,
                BlockingWaitStrategy.class.getName());

        Storage storage = createStorage(bufferSize, WaitStrategyName, readers.length, handlers, context);
        context.setStorage(storage);

        LOGGER.info("Transfer data from reader to writer...");

        RecordCollector rc = new RecordCollector(storage, metric, readerConfig.getFlowLimit());
        ExecutorService es = Executors.newFixedThreadPool(readers.length);
        CompletionService<Integer> cs = new ExecutorCompletionService<Integer>(es);
        for (int i = 0, len = readerConfigList.size(); i < len; i++) {
            cs.submit(new ReaderWorker(readers[i], context, readerConfigList.get(i), rc));
        }
        es.shutdown();

        metric.setReaderStartTime(System.currentTimeMillis());
        metric.setWriterStartTime(System.currentTimeMillis());
        while (!es.isTerminated()) {
            if (context.isWriterError()) {
                LOGGER.info("Write error.");
                LOGGER.info("Closing reader and writer.");
                // storage.close();
                closeReaders(readers);
                closeWriters(writers);
                LOGGER.info("Job run failed!");
                System.exit(JobStatus.FAILED.getStatus());
            }

            Utils.sleep(sleepMillis);
            LOGGER.info("Read: {}\tWrite: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }
        context.setReaderFinished(true);
        metric.setReaderEndTime(System.currentTimeMillis());

        while (!storage.isEmpty()) {
            if (context.isWriterError()) {
                LOGGER.info("Write error.");
                closeWriters(writers);
                LOGGER.info("Job run failed!");
                System.exit(JobStatus.FAILED.getStatus());
            }
            Utils.sleep(sleepMillis);
            LOGGER.info("Read Finished(total: {}), Write: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }

        storage.close();
        LOGGER.info("Read Finished(total: {}), Write Finished(total: {})", metric.getReadCount().get(), metric.getWriteCount().get());

        for (int i = 0, len = readers.length; i < len; i++) {
            try {
                Future<Integer> future = cs.take();
                if (future == null) {
                    LOGGER.info("Read error.");
                    closeWriters(writers);
                    LOGGER.info("Job run failed!");
                    System.exit(1);
                }

                Integer result = future.get();
                if (result == null) {
                    result = -1;
                }

                if (result != 0) {
                    LOGGER.info("Read error.");
                    closeWriters(writers);
                    LOGGER.info("Job run failed!");
                    System.exit(result);
                }
            } catch (Exception e) {
                LOGGER.error(Throwables.getStackTraceAsString(e));
                exitCode = 1;
            }
        }

        metric.setWriterEndTime(System.currentTimeMillis());
        if (!closeWriters(writers)) {
            exitCode = 1;
        }

        context.setWriterFinished(true);

        double readSeconds = (metric.getReaderEndTime() - metric.getReaderStartTime()) / 1000d;
        double writeSeconds = (metric.getWriterEndTime() - metric.getWriterStartTime()) / 1000d;
        String readSpeed = decimalFormat.format(metric.getReadCount().get() / readSeconds);
        String writeSpeed = decimalFormat.format(metric.getWriteCount().get() / writeSeconds);
        LOGGER.info("Read spent time: {}s, Write spent time: {}s", decimalFormat.format(readSeconds), decimalFormat.format(writeSeconds));
        LOGGER.info("Read records: {}/s, Write records: {}/s", readSpeed, writeSpeed);

        if ("jdbc".equals(readerName) || "jdbc".equals(writerName)) {
            DataSourceUtils.close();
        }

        System.exit(exitCode);
    }

    private Storage createStorage(int bufferSize, String WaitStrategyName, int producerCount, RecordWorkHandler[] handlers, JobContext context) {
        WaitStrategy waitStrategy = WaitStrategyFactory.build(WaitStrategyName);
        ExecutorService executorService = Executors.newCachedThreadPool();
        ProducerType producerType;
        if (producerCount == 1) {
            producerType = ProducerType.SINGLE;
        } else {
            producerType = ProducerType.MULTI;
        }
        Disruptor<RecordEvent> disruptor = new Disruptor<RecordEvent>(RecordEvent.FACTORY, bufferSize, executorService, producerType, waitStrategy);
        Storage storage = new Storage(disruptor, handlers, context);
        executorService.shutdown();
        return storage;
    }

    private boolean closeReaders(Reader[] readers) {
        try {
            for (Reader reader : readers) {
                reader.close();
            }

            return true;
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
        return false;
    }

    private boolean closeWriters(Writer[] writers) {
        try {
            for (Writer writer : writers) {
                writer.close();
            }

            return true;
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }

        return false;
    }

}
