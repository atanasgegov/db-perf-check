package com.akg.data.perf.comparisons.service;

import com.akg.data.perf.comparisons.config.Execution;

public abstract class AbstractCommander {
	
	public abstract int search(Execution execution);
	public abstract int insert(Execution execution);
	public abstract int update(Execution execution);
	public abstract int delete(Execution execution);
}
