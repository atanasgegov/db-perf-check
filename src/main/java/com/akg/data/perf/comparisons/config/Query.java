package com.akg.data.perf.comparisons.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Query implements Comparable<Query> {

	private String name;
	private String exec;
	private Integer percentage;
	private QueryParams[] params;
	private String additionalExec;

	@Override
	public int compareTo(Query o) {
		return Integer.compare(this.percentage, o.percentage);
	}
	
	public String toString() {
		return "name: "+name+", percentage: "+percentage;
	}
	
	public String toFullString() {
		return "name: "+name+", query: "+exec+", percentage: "+percentage;
	}
}
