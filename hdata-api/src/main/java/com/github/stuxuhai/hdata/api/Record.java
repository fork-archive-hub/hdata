package com.github.stuxuhai.hdata.api;

public interface Record {

	public void add(Object object);

	public void add(int index, Object object);

	public Object get(int index);

	public int size();
}
