package com.github.stuxuhai.hdata.api;

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
