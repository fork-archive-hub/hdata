/*
 * Author: wuya
 * Create Date: 2014年6月27日 下午4:02:30
 */
package com.github.stuxuhai.hdata.plugin.writer.csv;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringEscapeUtils;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.Fields;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.google.common.base.Preconditions;

/**
 * @author wuya
 *
 */
public class CSVWriter extends Writer {

    private String path = null;
    private String encoding = null;
    private String separator = null;
    private java.io.Writer writer;
    private CSVPrinter csvPrinter;
    private Fields fields;
    private boolean showColumns;
    private List<Object> csvList = new ArrayList<Object>();
    private static AtomicInteger sequence = new AtomicInteger(0);
    private static final Pattern REG_FILE_PATH_WITHOUT_EXTENSION = Pattern.compile(".*?(?=\\.\\w+$)");
    private static final Pattern REG_FILE_EXTENSION = Pattern.compile("(\\.\\w+)$");

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        path = writerConfig.getString(CSVWriterProperties.PATH);
        Preconditions.checkNotNull(path, "CSV writer required property: path");

        encoding = writerConfig.getString(CSVWriterProperties.ENCODING, "UTF-8");
        separator = StringEscapeUtils.unescapeJava(writerConfig.getString(CSVWriterProperties.SEPARATOR, ","));

        fields = context.getFields();
        showColumns = writerConfig.getBoolean(CSVWriterProperties.SHOW_COLUMNS, false);
        int parallelism = writerConfig.getParallelism();
        if (parallelism > 1) {
            String filePathWithoutExtension = "";
            String fileExtension = "";
            Matcher m1 = REG_FILE_PATH_WITHOUT_EXTENSION.matcher(path.trim());
            if (m1.find()) {
                filePathWithoutExtension = m1.group();
            }

            Matcher m2 = REG_FILE_EXTENSION.matcher(path.trim());
            if (m2.find()) {
                fileExtension = m2.group();
            }
            path = String.format("%s_%04d%s", filePathWithoutExtension, sequence.getAndIncrement(), fileExtension);
        }

        try {
            writer = new OutputStreamWriter(new FileOutputStream(path), encoding);
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(Record record) {
        if (csvPrinter == null) {
            try {
                csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withDelimiter(separator.charAt(0)));
                if (showColumns) {
                    for (Object object : fields) {
                        csvList.add(object);
                    }
                    csvPrinter.printRecord(csvList);
                    csvList.clear();
                }
            } catch (IOException e) {
                throw new HDataException(e);
            }
        }

        for (int i = 0, len = record.size(); i < len; i++) {
            csvList.add(record.get(i));
        }

        try {
            csvPrinter.printRecord(csvList);
        } catch (IOException e) {
            throw new HDataException(e);
        }
        csvList.clear();
    }

    @Override
    public void close() {
        if (csvPrinter != null) {
            try {
                csvPrinter.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
        }

        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
        }
    }

}
