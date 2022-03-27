package com.akg.data.perf.comparisons.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.akg.data.perf.comparisons.config.pojo.UseCases;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "commons")
@Data
public class CommonConfig {

	private String inputDataFile;
	private int batchSize;
	private long frequencyOutputInMs;
	private UseCases useCases;
}
