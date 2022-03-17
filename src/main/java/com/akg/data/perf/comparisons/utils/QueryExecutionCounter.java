package com.akg.data.perf.comparisons.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.annotation.Async;

import com.akg.data.perf.comparisons.config.Query;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExecutionCounter {

	private Long startTimeMillis;
	private Map<Query,Integer> queryCounter = new HashMap<>();
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	@Async
	public void printReccurently(long frequencyOutputInSec) {
		// Starting a recurrent thread
		Runnable threadPrintTask = () -> log.info(System.lineSeparator() + this.toString());
		scheduler.scheduleAtFixedRate(threadPrintTask, 0, frequencyOutputInSec, TimeUnit.MILLISECONDS);
	}
	
	public void shutdownReccurentPrint() {
		scheduler.shutdown();
	}

	public QueryExecutionCounter(long startTimeMillis, List<Query> queries) {
		this.startTimeMillis = startTimeMillis;
		queries.stream().forEach( t -> queryCounter.put(t, 0) );
	}

	public void increment(Query key) {
		Integer incrementedVal = Integer.valueOf( queryCounter.get(key) + 1 ) ;
		queryCounter.put(key, incrementedVal);
	}

	public void increment(Query key, int value) {
		Integer incrementedVal = Integer.valueOf( queryCounter.get(key) + value ) ;
		queryCounter.put(key, incrementedVal);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("QUERY,EXECUTIONS,EPS").append(System.lineSeparator());
		double elapsedTimeInSec = (double)(System.currentTimeMillis()-startTimeMillis)/1000;
		int[] total= new int[1];
		queryCounter.forEach((k,v)->{
			str.append(k.getName()).append(",").append(v).append(",").append( (double)v/elapsedTimeInSec ).append(System.lineSeparator());
			total[0] = total[0] + v;
		});
		str.append("TOTAL,").append(total[0]).append(",").append(total[0]/elapsedTimeInSec).append(System.lineSeparator());
		str.append("Execution time: ").append(elapsedTimeInSec);
		return str.toString();
	}
}