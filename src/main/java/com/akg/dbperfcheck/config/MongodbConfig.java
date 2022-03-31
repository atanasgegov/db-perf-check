package com.akg.dbperfcheck.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Configuration("mongodbConfig")
@ConfigurationProperties(prefix = "mongodb")
@Getter
@Setter
@Slf4j
public class MongodbConfig extends AbstractConfig {

	private String database;
	private String collection;

	@Bean
	public MongoClient createMongoClient() {
		String mongodbUrl = String.format("mongodb://%s:%s@%s:%d/%s", user, pass, host, port, database);
		try {
			return MongoClients.create(mongodbUrl);
		} catch( Exception e ) {
			log.error(e.getMessage());
			return null;
		}
	}
}
