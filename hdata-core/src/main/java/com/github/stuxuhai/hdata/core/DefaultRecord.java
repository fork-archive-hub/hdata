package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.Record;

public class DefaultRecord implements Record {

	private final Object[] fields;
	private int cursor;

	public DefaultRecord(int fieldCount) {
		fields = new Object[fieldCount];
	}

	@Override
	public void add(int index, Object field) {
		fields[index] = field;
		this.cursor++;
	}

	@Override
	public void add(Object field) {
		add(cursor, field);
	}

	@Override
	public Object get(int index) {
		return fields[index];
	}

	@Override
	public int size() {
		return fields.length;
	}
}
