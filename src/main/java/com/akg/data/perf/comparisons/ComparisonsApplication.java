package com.akg.data.perf.comparisons;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication
public class ComparisonsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ComparisonsApplication.class, args);
	}

}
