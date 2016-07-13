package com.github.stuxuhai.hdata.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Throwables;

public class PluginLoader {

    private static Map<String, String> readerMap;
    private static Map<String, String> writerMap;

    public static String getReaderClassName(String name) {
        return readerMap.get(name);
    }

    public static String getWriterClassName(String name) {
        return writerMap.get(name);
    }

    static {
        readerMap = new HashMap<String, String>();
        writerMap = new HashMap<String, String>();

        String path = Utils.getConfigDir() + Constants.PLUGINS_XML;
        try {
            XMLConfiguration config = new XMLConfiguration(path);
            config.setValidating(true);

            List<HierarchicalConfiguration> readerList = config.configurationsAt("readers.reader");
            for (HierarchicalConfiguration hc : readerList) {
                String name = hc.getString("name");
                String clazz = hc.getString("class");
                readerMap.put(name, clazz);
            }

            List<HierarchicalConfiguration> writerList = config.configurationsAt("writers.writer");
            for (HierarchicalConfiguration hc : writerList) {
                String name = hc.getString("name");
                String clazz = hc.getString("class");
                writerMap.put(name, clazz);
            }
        } catch (ConfigurationException e) {
            Throwables.propagate(e);
        }
    }
}