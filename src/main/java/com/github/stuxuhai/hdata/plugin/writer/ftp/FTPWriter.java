/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.writer.ftp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.net.ftp.FTPClient;

import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.github.stuxuhai.hdata.util.FTPUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class FTPWriter extends Writer {

    private String host;
    private int port;
    private String username;
    private String password;
    private String fieldsSeparator;
    private String lineSeparator;
    private String encoding;
    private String path;
    private boolean gzipCompress;
    private FTPClient ftpClient;
    private BufferedWriter bw;
    private String[] strArray;
    private static AtomicInteger sequence = new AtomicInteger(0);
    private static final Pattern REG_FILE_PATH_WITHOUT_EXTENSION = Pattern.compile(".*?(?=\\.\\w+$)");
    private static final Pattern REG_FILE_EXTENSION = Pattern.compile("(\\.\\w+)$");

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        host = writerConfig.getString(FTPWriterProperties.HOST);
        Preconditions.checkNotNull(host, "FTP writer required property: host");

        port = writerConfig.getInt(FTPWriterProperties.PORT, 21);
        username = writerConfig.getString(FTPWriterProperties.USERNAME, "anonymous");
        password = writerConfig.getString(FTPWriterProperties.PASSWORD, "");
        fieldsSeparator = StringEscapeUtils.unescapeJava(writerConfig.getString(FTPWriterProperties.FIELDS_SEPARATOR, "\t"));
        lineSeparator = StringEscapeUtils.unescapeJava(writerConfig.getString(FTPWriterProperties.LINE_SEPARATOR, "\n"));
        encoding = writerConfig.getString(FTPWriterProperties.ENCODING, "UTF-8");
        path = writerConfig.getString(FTPWriterProperties.PATH);
        Preconditions.checkNotNull(path, "FTP writer required property: path");

        gzipCompress = writerConfig.getBoolean(FTPWriterProperties.GZIP_COMPRESS, false);

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
            ftpClient = FTPUtils.getFtpClient(host, port, username, password);
            OutputStream outputStream = ftpClient.storeFileStream(path);
            if (gzipCompress) {
                bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(outputStream), encoding));
            } else {
                bw = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
            }
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(Record record) {
        if (strArray == null) {
            strArray = new String[record.size()];
        }

        for (int i = 0, len = record.size(); i < len; i++) {
            Object o = record.get(i);
            if (o == null) {
                strArray[i] = "NULL";
            } else {
                strArray[i] = o.toString();
            }
        }
        try {
            bw.write(Joiner.on(fieldsSeparator).join(strArray));
            bw.write(lineSeparator);
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void close() {
        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
        }
        FTPUtils.closeFtpClient(ftpClient);
    }
}
