/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author: wuya
 * Create Date: 2014年7月2日 下午3:48:21
 */
package com.github.stuxuhai.hdata.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobContext;
import com.google.common.base.Throwables;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * @author wuya
 *
 */
public class RecordEventExceptionHandler implements ExceptionHandler<Object> {

    private final Disruptor<RecordEvent> disruptor;
    private final JobContext context;
    private static Logger LOGGER = LogManager.getLogger(RecordEventExceptionHandler.class);

    public RecordEventExceptionHandler(Disruptor<RecordEvent> disruptor, JobContext context) {
        this.disruptor = disruptor;
        this.context = context;
    }

    public void handleEventException(Throwable t, long sequence, Object event) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        context.setWriterError(true);
        disruptor.shutdown();
    }

    public void handleOnShutdownException(Throwable t) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        disruptor.shutdown();
    }

    public void handleOnStartException(Throwable t) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        disruptor.shutdown();
    }
}
