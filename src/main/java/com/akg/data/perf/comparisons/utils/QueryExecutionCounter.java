package com.akg.data.perf.comparisons.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akg.data.perf.comparisons.config.Query;

public class QueryExecutionCounter {

	private long startTimeMillis;
	private Map<Query,Integer> queryCounter = new HashMap<>();

	public QueryExecutionCounter(long startTimeMillis, List<Query> queries) {
		this.startTimeMillis = startTimeMillis;
		queries.stream().forEach( t -> queryCounter.put(t, 0) );
	}

	public void increment(Query key) {
		Integer incrementedVal = Integer.valueOf( queryCounter.get(key) + 1 ) ;
		queryCounter.put(key, incrementedVal);
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("QUERY,EXECUTIONS,EPS").append(System.lineSeparator());
		int[] total= new int[1];
		double elapsedTimeInSec = (System.currentTimeMillis()-startTimeMillis)/1000;
		queryCounter.forEach((k,v)->{
			str.append(k.getName()).append(",").append(v).append(",").append( (double)v/elapsedTimeInSec ).append(System.lineSeparator());
			total[0] = total[0] + v;
		});
		str.append("TOTAL,").append(total[0]).append(",").append((double)total[0]/elapsedTimeInSec);
		return str.toString();
	}
}