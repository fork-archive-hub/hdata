package com.github.stuxuhai.hdata.core;

import java.net.URL;
import java.net.URLClassLoader;

public class PluginClassLoader extends URLClassLoader {

    public PluginClassLoader(URL[] urls) {
        super(urls, PluginClassLoader.class.getClassLoader());
    }
}
