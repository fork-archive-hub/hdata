/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.plugin.Record;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0, len = fields.length; i < len; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fields[i]);
        }
        sb.append("}");
        return sb.toString();
    }

}
