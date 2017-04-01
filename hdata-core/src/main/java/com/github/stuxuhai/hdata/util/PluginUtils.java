package com.github.stuxuhai.hdata.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.core.PluginClassLoader;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PluginUtils {

    private static Map<String, PluginClassLoader> cache = Maps.newConcurrentMap();
    private static final Logger LOGGER = LogManager.getLogger();

    private static List<URL> listFileByPluginName(String pluginName) throws MalformedURLException {
        List<URL> result = Lists.newArrayList();
        File file = new File(PluginUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath().replaceAll("/lib/.*\\.jar", "")
                + "/plugins/" + pluginName);
        if (!file.exists()) {
            throw new HDataException("Plugin not found: " + pluginName);
        }

        File[] jars = file.listFiles();
        for (File jar : jars) {
            result.add(jar.toURI().toURL());
        }
        return result;
    }

    public static Class<?> loadClass(String pluginName, String className) throws ClassNotFoundException, MalformedURLException {
        List<URL> list = listFileByPluginName(pluginName);
        PluginClassLoader classLoader = cache.get(pluginName);
        if (classLoader == null) {
            classLoader = new PluginClassLoader(list.toArray(new URL[list.size()]));
            cache.put(pluginName, classLoader);
        }
        return classLoader.loadClass(className);
    }

    public static void closeURLClassLoader() {
        for (Entry<String, PluginClassLoader> entry : cache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }
    }
}
