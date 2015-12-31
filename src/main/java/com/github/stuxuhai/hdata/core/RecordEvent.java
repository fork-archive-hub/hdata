/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.plugin.Record;
import com.lmax.disruptor.EventFactory;

public class RecordEvent {

    private Record record;

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public static final EventFactory<RecordEvent> FACTORY = new EventFactory<RecordEvent>() {

        @Override
        public RecordEvent newInstance() {
            return new RecordEvent();
        }
    };

}
