/*
 * Author: wuya
 * Create Date: 2014年6月27日 下午2:34:46
 */
package com.github.stuxuhai.hdata.plugin.reader.csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

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
public class CSVReader extends Reader {

    private String path = null;
    private int startRow = 1;
    private String encoding = null;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        path = readerConfig.getString(CSVReaderProperties.PATH);
        startRow = readerConfig.getInt(CSVReaderProperties.START_ROW, 1);
        encoding = readerConfig.getString(CSVReaderProperties.ENCODING, "UTF-8");
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        long currentRow = 0;
        try {
            java.io.Reader in = new InputStreamReader(new FileInputStream(path), encoding);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
            for (CSVRecord csvRecord : records) {
                currentRow++;
                if (currentRow >= startRow) {
                    Record hdataRecord = new DefaultRecord(csvRecord.size());
                    for (int i = 0, len = csvRecord.size(); i < len; i++) {
                        hdataRecord.add(csvRecord.get(i));
                    }
                    recordCollector.send(hdataRecord);
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new HDataException(e);
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public Class<? extends Splitter> getSplitterClass() {
        return CSVSplitter.class;
    }
}
