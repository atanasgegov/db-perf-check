package com.akg.data.perf.comparisons.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.config.pojo.UseCases;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "commons")
@Data
public class Config {

	private String inputDataFile;
	private int batchSize;
	private long frequencyOutputInMs;
	private UseCases useCases;
	private List<Query> searchQueries;
	private List<Query> updateQueries;
	private List<Query> deleteQueries;
	
	public enum ExecutionMode {

		INSERTS("inserts"),
	    SEARCH("search"),
		UPDATES("updates"),
		DELETES("deletes"); 

		private final String mode;
	    private ExecutionMode(String mode) {
	        this.mode = mode;
	    }

	    public String getValue() {
			return mode;
		}
	}
	
	public enum Technology {

		ELASTICSEARCH("elasticsearch"),
		MONGODB("mongodb"); 

		private final String name;
	    private Technology(String name) {
	        this.name = name;
	    }

	    public String getValue() {
			return name;
		}
	}
}
