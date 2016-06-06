package com.github.stuxuhai.hdata.api;

import java.util.ArrayList;
import java.util.Collections;

public class Fields extends ArrayList<String> {

	private static final long serialVersionUID = -174064216143075549L;

	public Fields() {
		super();
	}

	public Fields(String... fields) {
		super();
		Collections.addAll(this, fields);
	}

}
