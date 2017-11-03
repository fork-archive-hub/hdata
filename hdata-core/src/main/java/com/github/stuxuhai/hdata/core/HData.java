package com.github.stuxuhai.hdata.core;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.EngineConfig;
import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.JobStatus;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.api.Storage;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.common.HDataConfigConstants;
import com.github.stuxuhai.hdata.config.DefaultEngineConfig;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Throwables;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class HData {

    private static final Logger LOGGER = LogManager.getLogger(HData.class);

    private int exitCode = 0;

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

        final EngineConfig engineConfig = DefaultEngineConfig.create();
        context.setEngineConfig(engineConfig);

        long sleepMillis = engineConfig.getLong(HDataConfigConstants.HDATA_SLEEP_MILLIS, Constants.DEFAULT_HDATA_SLEEP_MILLIS);

        List<PluginConfig> readerConfigList = null;
        final Splitter splitter = jobConfig.newSplitter();
        if (splitter != null) {
            LOGGER.info("Executing splitter for reader.");

            ExecutorService es = Executors.newCachedThreadPool();
            Callable<List<PluginConfig>> callable = new Callable<List<PluginConfig>>() {
                @Override
                public List<PluginConfig> call() throws Exception {
                    Thread.currentThread().setContextClassLoader(splitter.getClass().getClassLoader());
                    return splitter.split(jobConfig);
                }
            };

            Future<List<PluginConfig>> future = es.submit(callable);
            es.shutdown();
            try {
                readerConfigList = future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("", e);
                System.exit(JobStatus.FAILED.getStatus());
            }

            if (readerConfigList.isEmpty()) {
                System.exit(JobStatus.SUCCESS.getStatus());
            }
        } else {
            if (readerConfig.getParallelism() > 1) {
                LOGGER.warn("Can not find splitter, reader parallelism is set to 1.");
            }
            readerConfigList = new ArrayList<>();
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

        RecordCollector rc = new DefaultRecordCollector(storage, metric, readerConfig.getFlowLimit());
        ExecutorService es = Executors.newFixedThreadPool(readers.length);
        CompletionService<Integer> cs = new ExecutorCompletionService<>(es);
        for (int i = 0, len = readerConfigList.size(); i < len; i++) {
            ReaderWorker readerWorker = new ReaderWorker(readers[i], context, readerConfigList.get(i), rc);
            cs.submit(readerWorker);
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
				final DecimalFormat decimalFormat = new DecimalFormat("#0.00");
        String readSpeed = decimalFormat.format(metric.getReadCount().get() / readSeconds);
        String writeSpeed = decimalFormat.format(metric.getWriteCount().get() / writeSeconds);
        LOGGER.info("Read spent time: {}s, Write spent time: {}s", decimalFormat.format(readSeconds), decimalFormat.format(writeSeconds));
        LOGGER.info("Read records: {}/s, Write records: {}/s", readSpeed, writeSpeed);

        // PluginUtils.closeURLClassLoader();

        System.exit(exitCode);
    }

    private Storage createStorage(int bufferSize, String WaitStrategyName, int producerCount, RecordWorkHandler[] handlers,
            JobContext context) {
        WaitStrategy waitStrategy = WaitStrategyFactory.build(WaitStrategyName);
        ProducerType producerType;
        if (producerCount == 1) {
            producerType = ProducerType.SINGLE;
        } else {
            producerType = ProducerType.MULTI;
        }
        Disruptor<RecordEvent> disruptor = new Disruptor<>(RecordEvent.FACTORY, bufferSize, Executors.defaultThreadFactory(), producerType,
                waitStrategy);
        Storage storage = new DefaultStorage(disruptor, handlers, context);
        return storage;
    }

    private boolean closeReaders(final Reader[] readers) {
        ExecutorService es = Executors.newCachedThreadPool();
        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setContextClassLoader(readers[0].getClass().getClassLoader());
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
        };

        Future<Boolean> future = es.submit(callable);
        es.shutdown();

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            return false;
        }
    }

    private boolean closeWriters(final Writer[] writers) {
        ExecutorService es = Executors.newCachedThreadPool();
        Callable<Boolean> callable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setContextClassLoader(writers[0].getClass().getClassLoader());
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
        };

        Future<Boolean> future = es.submit(callable);
        es.shutdown();

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            return false;
        }
    }
}
