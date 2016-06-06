package com.github.stuxuhai.hdata.api;

public class PluginConfig extends Configuration {

	private static final String PARALLELISM_KEY = "parallelism";
	private static final int DEFAULT_PARALLELISM = 1;

	private static final String FLOWLIMIT_KEY = "flow.limit";
	private static final long DEFAULT_FLOWLIMIT = 0;

	private static final long serialVersionUID = 3311331304791946068L;

	public PluginConfig() {
		super();
	}

	public int getParallelism() {
		int parallelism = getInt(PARALLELISM_KEY, DEFAULT_PARALLELISM);
		if (parallelism < 1) {
			throw new IllegalArgumentException("Reader and Writer parallelism must be >= 1.");
		}
		return parallelism;
	}

	public long getFlowLimit() {
		long flowLimit = getLong(FLOWLIMIT_KEY, DEFAULT_FLOWLIMIT);
		if (flowLimit < 0) {
			throw new IllegalArgumentException("Reader and Writer FLowLimit must be >= 0.");
		}
		return flowLimit;
	}
}
