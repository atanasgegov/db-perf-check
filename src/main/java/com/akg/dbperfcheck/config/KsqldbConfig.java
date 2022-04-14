package com.akg.dbperfcheck.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.akg.dbperfcheck.config.pojo.Query;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import lombok.Getter;
import lombok.Setter;

@Configuration("ksqldbConfig")
@ConfigurationProperties(prefix = "ksqldb")
@Getter
@Setter
public class KsqldbConfig extends AbstractConfig  {
	
	protected Query cleanQuery;
	
	@Bean
	public Client getKsqlDBClient() {
		ClientOptions options = ClientOptions.create()
				  .setHost(host)
				  .setPort(port);

		return Client.create(options);		
	}
}
