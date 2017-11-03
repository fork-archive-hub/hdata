package com.github.stuxuhai.hdata.plugin.wit;

import com.github.stuxuhai.hdata.api.Record;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zqq90
 */
public class WitRecord implements Record {

    private final List<Object> datas;

    public WitRecord() {
        this.datas = new ArrayList<>();
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
            // XXX: should throw IndexOutOfBoundsException ?
            return null;
        }
        return this.datas.get(index);
    }

    @Override
    public int size() {
        return this.datas.size();
    }

}
