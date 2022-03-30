package com.akg.data.perf.comparisons.config;

import javax.sql.DataSource;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Configuration("postgresConfig")
@ConfigurationProperties(prefix = "postgres")
@Getter
@Setter
@Slf4j
public class PostgresConfig extends AbstractConfig {

	private String url;

	@Bean
    public DataSource getDataSource() {
		try {
	        DriverManagerDataSource dataSource = new DriverManagerDataSource();
	        dataSource.setUrl(url);
	        dataSource.setUsername(user);
	        dataSource.setPassword(pass);
	        return dataSource;
		} catch( Exception e ) {
			log.error(e.getMessage());
			return null;
		}
    }
}