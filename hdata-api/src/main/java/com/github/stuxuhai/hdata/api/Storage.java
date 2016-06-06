package com.github.stuxuhai.hdata.api;

public interface Storage {

	public void put(Record record);

	public void put(Record[] records);

	public boolean isEmpty();

	public int size();

	public void close();
}
