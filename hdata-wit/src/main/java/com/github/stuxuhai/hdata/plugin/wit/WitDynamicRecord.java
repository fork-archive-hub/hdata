package com.github.stuxuhai.hdata.plugin.wit;

import com.github.stuxuhai.hdata.api.Record;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zqq90
 */
public class WitDynamicRecord implements Record {

    private final List<Object> datas;

    public WitDynamicRecord() {
        this.datas = new ArrayList<>();
    }

    public WitDynamicRecord(Record record) {
        List<Object> list = new ArrayList<>();
        for (int i = 0, len = record.size(); i < len; i++) {
            list.add(record.get(i));
        }
        this.datas = list;
    }

    @Override
    public void add(Object object) {
        this.datas.add(object);
    }

    @Override
    public void add(int index, Object object) {
        final int size = this.datas.size();
        if (index >= size) {
            for (int i = index - size; i != 0; i--) {
                this.datas.add(null);
            }
            this.datas.add(object);
        } else {
            this.datas.set(index, object);
        }
    }

    @Override
    public Object get(int index) {
        if (index >= this.datas.size()) {
            return null;
        }
        return this.datas.get(index);
    }

    @Override
    public int size() {
        return this.datas.size();
    }

}
