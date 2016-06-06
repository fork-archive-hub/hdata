package com.github.stuxuhai.hdata.api;

public interface RecordCollector {

	public void send(Record record);

	public void send(Record[] records);
}
