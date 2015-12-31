/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

public class OutputFieldsDeclarer {
    private Fields fields;

    public void declare(Fields fields) {
        this.fields = fields;
    }

    public Fields getFields() {
        return fields;
    }
}
