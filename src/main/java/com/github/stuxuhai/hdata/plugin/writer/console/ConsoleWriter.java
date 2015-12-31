/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.writer.console;

import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.Writer;

public class ConsoleWriter extends Writer {

    @Override
    public void execute(Record record) {
        System.out.println(record);
    }
}
