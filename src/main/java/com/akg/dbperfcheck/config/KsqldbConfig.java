package com.akg.dbperfcheck.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.akg.dbperfcheck.config.pojo.Query;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Configuration("ksqldbConfig")
@ConfigurationProperties(prefix = "ksqldb")
@Getter
@Setter
@Slf4j
public class KsqldbConfig extends AbstractConfig {
	
	private Query cleanQuery;
	
	@Bean
	public Client getKsqlDBClient() {
		try {
			ClientOptions options = ClientOptions.create()
					  .setHost(host)
					  .setPort(port);
	
			return Client.create(options);
		} catch( Exception e ) {
			log.error(e.getMessage());
			return null;
		}
	}
}
