package com.akg.dbperfcheck.config;

import java.net.InetSocketAddress;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Configuration("cassandraConfig")
@ConfigurationProperties(prefix = "cassandra")
@Getter
@Setter
@Slf4j
public class CassandraConfig extends AbstractConfig {

	private String datacenter;

	@Bean
	public CqlSession getCassandraSession() {
		try {
			DriverConfigLoader driverConfigLoader =
				    DriverConfigLoader.programmaticBuilder()
				        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
				        //.withDuration(DefaultDriverOption, null)
				        .endProfile()
				        .build();
			
			return CqlSession.builder()
				    .addContactPoint(new InetSocketAddress(host, port))
				    .withAuthCredentials(user, pass)
				    .withConfigLoader( driverConfigLoader )
				    .withLocalDatacenter(datacenter)
				    .build();
		} catch( Exception e) {
			log.warn( e.getMessage() );
			return null;
		}
	}
}
