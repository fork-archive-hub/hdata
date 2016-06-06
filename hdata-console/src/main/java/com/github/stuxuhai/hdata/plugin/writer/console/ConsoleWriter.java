package com.github.stuxuhai.hdata.plugin.writer.console;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;

public class ConsoleWriter extends Writer {

	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void execute(Record record) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (int i = 0, len = record.size(); i < len; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			Object obj = record.get(i);
			if (obj instanceof Timestamp) {
				sb.append(dateFormat.format(obj));
			} else {
				sb.append(obj);
			}
		}
		sb.append("}");
		System.out.println(sb.toString());
	}
}
