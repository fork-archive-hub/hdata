/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin;

public interface Record {

    public void add(Object object);

    public void add(int index, Object object);

    public Object get(int index);

    public int size();
}
