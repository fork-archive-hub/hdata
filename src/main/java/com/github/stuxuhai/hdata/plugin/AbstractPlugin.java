/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin;

public abstract class AbstractPlugin implements Pluginable {

    private String pluginName;

    @Override
    public String getPluginName() {
        return this.pluginName;
    }

    @Override
    public void setPluginName(String name) {
        this.pluginName = name;
    }

}
