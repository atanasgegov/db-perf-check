package com.akg.data.perf.comparisons.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.akg.data.perf.comparisons.config.pojo.Query;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "mongodb")
@Data
public class MongodbConfig {

	private String host;
	private int port;
	private String user;
	private String pass;
	private String database;
	private String collection;
	private List<Query> searchQueries;
	private List<Query> updateQueries;
	private List<Query> deleteQueries;

	@Bean
	public MongoClient createMongoClient() {
		String mongodbUrl = String.format("mongodb://%s:%s@%s:%d/%s", user, pass, host, port, database);
		return MongoClients.create(mongodbUrl);
	}
}
