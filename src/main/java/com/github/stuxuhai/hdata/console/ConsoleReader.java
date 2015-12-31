/*
 * Author: wuya
 * Create Date: 2014年6月27日 上午10:44:19
 */
package com.github.stuxuhai.hdata.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.RecordCollector;
import com.github.stuxuhai.hdata.plugin.Splitter;

/**
 * @author wuya
 *
 */
public class ConsoleReader extends Reader {

    private BufferedReader br = null;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        br = new BufferedReader(new InputStreamReader(System.in));
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            String line = null;
            while ((line = br.readLine()) != null) {
                Record record = new DefaultRecord(1);
                record.add(line);
                recordCollector.send(record);
            }
            br.close();
        } catch (IOException e) {
            new HDataException(e);
        }
    }

    @Override
    public Class<? extends Splitter> getSplitterClass() {
        return null;
    }

}
