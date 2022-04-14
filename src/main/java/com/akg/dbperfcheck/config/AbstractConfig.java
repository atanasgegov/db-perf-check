package com.akg.dbperfcheck.config;

import java.util.List;

import com.akg.dbperfcheck.config.pojo.Query;

import lombok.Data;

@Data
public abstract class AbstractConfig {

	protected String host;
	protected int port;
	protected String user;
	protected String pass;
	protected Query maxIdQuery;
	protected List<Query> searchQueries;
	protected List<Query> updateQueries;
	protected List<Query> deleteQueries;

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
		MONGODB("mongodb"),
		CASSANDRA("cassandra"); 

		private final String name;
	    private Technology(String name) {
	        this.name = name;
	    }

	    public String getValue() {
			return name;
		}
	}
}
