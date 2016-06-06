package com.github.stuxuhai.hdata.api;

public class OutputFieldsDeclarer {
	private Fields fields;

	public void declare(Fields fields) {
		this.fields = fields;
	}

	public Fields getFields() {
		return fields;
	}
}
