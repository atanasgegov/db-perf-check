package com.akg.data.perf.comparisons.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "commons")
@Data
public class Config {

	private String inputDataFile;
	private int batchSize;
	private long frequencyOutputInMs;
	private UseCases useCases;
	private String activeUseCase = UseCase.ONE.getValue();
	
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

		ELASTICSEARCH("elasticsearch"); 

		private final String name;
	    private Technology(String name) {
	        this.name = name;
	    }

	    public String getValue() {
			return name;
		}
	}

	public enum UseCase {

		ONE("one"),
	    CRUD("crud"); 

		private final String name;
	    private UseCase(String name) {
	        this.name = name;
	    }

	    public String getValue() {
			return name;
		}
	}
}
