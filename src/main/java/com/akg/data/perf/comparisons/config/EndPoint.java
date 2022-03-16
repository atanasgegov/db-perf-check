package com.akg.data.perf.comparisons.config;

import lombok.Data;

@Data
public class EndPoint {
	
	private String search;
	private String insert;
	private String update;
	private String delete;
}
