/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.config;

import java.util.Iterator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import com.github.stuxuhai.hdata.core.PluginLoader;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.Splitter;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class JobConfig extends Configuration {

    private final PluginConfig readerConfig;
    private final PluginConfig writerConfig;
    private final String readerName;
    private final String writerName;
    private static final long serialVersionUID = 1L;

    public JobConfig(String readerName, PluginConfig readerConfig, String writerName, PluginConfig writerConfig) {
        super();
        this.readerName = readerName;
        this.readerConfig = readerConfig;
        this.writerName = writerName;
        this.writerConfig = writerConfig;
    }

    public PluginConfig getReaderConfig() {
        return readerConfig;
    }

    public PluginConfig getWriterConfig() {
        return writerConfig;
    }

    public String getReaderName() {
        return readerName;
    }

    public String getWriterName() {
        return writerName;
    }

    public Reader newReader() {
        String readerClassName = PluginLoader.getReaderClassName(readerName);
        Preconditions.checkNotNull(readerClassName, "Can not find class for reader: " + readerName);

        try {
            return (Reader) Class.forName(readerClassName).newInstance();
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }

    public Splitter newSplitter() {
        Reader reader = newReader();
        try {
            Class<? extends Splitter> splitterClass = reader.getSplitterClass();
            if (splitterClass == null) {
                return null;
            }
            return splitterClass.newInstance();
        } catch (InstantiationException e) {
            throw new HDataException(e);
        } catch (IllegalAccessException e) {
            throw new HDataException(e);
        }
    }

    public Writer newWriter() {
        String writerClassName = PluginLoader.getWriterClassName(writerName);
        Preconditions.checkNotNull(writerClassName, "Can not find class for writer: " + writerName);

        try {
            return (Writer) Class.forName(writerClassName).newInstance();
        } catch (Exception e) {
            throw new HDataException("Can not create new writer instance for: " + writerName, e);
        }
    }

    public static JobConfig createFromXML(String path) {
        try {
            XMLConfiguration xmlConfig = new XMLConfiguration(path);
            xmlConfig.setValidating(true);

            PluginConfig readerPluginConfig = new PluginConfig();
            String readerName = xmlConfig.getString("reader[@name]");
            SubnodeConfiguration readerSc = xmlConfig.configurationAt("reader");
            Iterator<String> readerIt = readerSc.getKeys();
            while (readerIt.hasNext()) {
                String key = readerIt.next();
                if (!key.startsWith("[@")) {
                    readerPluginConfig.setProperty(key.replace("..", "."), readerSc.getString(key));
                }
            }

            PluginConfig writerPluginConfig = new PluginConfig();
            String writerName = xmlConfig.getString("writer[@name]");
            SubnodeConfiguration writerSc = xmlConfig.configurationAt("writer");
            Iterator<String> writerIt = writerSc.getKeys();
            while (writerIt.hasNext()) {
                String key = writerIt.next();
                if (!key.startsWith("[@")) {
                    writerPluginConfig.setProperty(key.replace("..", "."), writerSc.getString(key));
                }
            }

            return new JobConfig(readerName, readerPluginConfig, writerName, writerPluginConfig);
        } catch (ConfigurationException e) {
            Throwables.propagate(e);
        }

        return null;
    }
}
