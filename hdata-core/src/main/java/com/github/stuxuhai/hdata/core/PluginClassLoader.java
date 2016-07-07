/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2016 All Rights Reserved.
 *
 * Author: wuya
 * Create Date: 2016年4月10日 下午4:42:43
 */

package com.github.stuxuhai.hdata.core;

import java.net.URL;
import java.net.URLClassLoader;

public class PluginClassLoader extends URLClassLoader {

    public PluginClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name.startsWith("com.github.stuxuhai.hdata.api.") || name.startsWith("org.apache.logging.") || name.startsWith("org.apache.log4j.")
                || name.startsWith("org.slf4j.") || name.startsWith("org.apache.commons.logging.")) {
            return getClass().getClassLoader().loadClass(name);
        } else {
            return super.loadClass(name);
        }
    }

}
