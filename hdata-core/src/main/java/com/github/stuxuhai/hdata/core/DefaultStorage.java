package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Storage;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DefaultStorage implements Storage {

	private final Disruptor<RecordEvent> disruptor;
	private final RingBuffer<RecordEvent> ringBuffer;

	private static final EventTranslatorOneArg<RecordEvent, Record> TRANSLATOR = new EventTranslatorOneArg<RecordEvent, Record>() {

		@Override
		public void translateTo(RecordEvent event, long sequence, Record record) {
			event.setRecord(record);
		}
	};

	public DefaultStorage(Disruptor<RecordEvent> disruptor, RecordWorkHandler[] handlers, JobContext context) {
		this.disruptor = disruptor;
		disruptor.setDefaultExceptionHandler(new RecordEventExceptionHandler(disruptor, context));
		disruptor.handleEventsWithWorkerPool(handlers);
		ringBuffer = disruptor.start();
	}

	@Override
	public void put(Record record) {
		disruptor.publishEvent(TRANSLATOR, record);
	}

	@Override
	public void put(Record[] records) {
		for (Record record : records) {
			put(record);
		}
	}

	@Override
	public boolean isEmpty() {
		return ringBuffer.remainingCapacity() == ringBuffer.getBufferSize();
	}

	@Override
	public int size() {
		return ringBuffer.getBufferSize();
	}

	@Override
	public void close() {
		disruptor.shutdown();
	}
}
