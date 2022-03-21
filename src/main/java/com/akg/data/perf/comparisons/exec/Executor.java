package com.akg.data.perf.comparisons.exec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.akg.data.perf.comparisons.config.Config;
import com.akg.data.perf.comparisons.config.ElasticsearchConfig;
import com.akg.data.perf.comparisons.config.MongodbConfig;
import com.akg.data.perf.comparisons.config.pojo.Execution;
import com.akg.data.perf.comparisons.config.pojo.UseCases;
import com.akg.data.perf.comparisons.service.ElasticsearchCommander;
import com.akg.data.perf.comparisons.service.MongodbCommander;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class Executor {
	
	@Autowired
	private Config config;
	
	@Autowired
	private ElasticsearchConfig esConfig;
	@Autowired
	private MongodbConfig mongodbConfig;
	
	@Autowired
	private ElasticsearchCommander elasticsearchCommander;
	@Autowired
	private MongodbCommander mongodbCommander;
	
	@EventListener(ApplicationReadyEvent.class)
	@Order(1)
	public void exec() {

		long start = System.currentTimeMillis();
		if( config.getUseCases().getActiveUseCase().equals( UseCases.Type.ONE ) ) {

			this.run( config.getUseCases().getOne() );
		} else if( config.getUseCases().getActiveUseCase().equals( UseCases.Type.CRUD ) ) {

			for(Execution execution : config.getUseCases().getCrud()) {
				this.run(execution);
			}
		} else {
			log.warn("Please, check your configuration, probably the wrong value was set for active-use-case: {}", config.getUseCases().getActiveUseCase());		
		}

		long end = System.currentTimeMillis();
		log.info("Elapsed Time in seconds: "+ (double)(end-start)/1000);
		System.exit(0);
	}

	private void run( Execution execution ) {

		String what = execution.getWhat();
		String mode = execution.getMode();
		log.info( "Executing '{}' '{}' queries for {} ms ...", what, mode, execution.getTimeInMs() );
		if( what.equals( Config.Technology.ELASTICSEARCH.getValue() ) ) {
			if( mode.equals( Config.ExecutionMode.SEARCH.getValue() ) ) {
				elasticsearchCommander.search(execution, esConfig.getSearchQueries());
			} else if( mode.equals( Config.ExecutionMode.INSERTS.getValue() ) ) {
				elasticsearchCommander.insert(execution);
			} else if( mode.equals( Config.ExecutionMode.UPDATES.getValue() ) ) {
				elasticsearchCommander.update(execution, esConfig.getUpdateQueries());
			} else if( mode.equals( Config.ExecutionMode.DELETES.getValue() ) ) {
				elasticsearchCommander.delete(execution, esConfig.getDeleteQueries());
			} else {
				log.warn("Wrong execution.mode value: '{}' for execution.what='{}' in the configuration file.", mode, what);
			}
		} else if(what.equals( Config.Technology.MONGODB.getValue())) {
			if( mode.equals( Config.ExecutionMode.SEARCH.getValue() ) ) {
				mongodbCommander.search(execution, mongodbConfig.getSearchQueries());
			} else if( mode.equals( Config.ExecutionMode.INSERTS.getValue() ) ) {
				mongodbCommander.insert(execution);
			} else if( mode.equals( Config.ExecutionMode.UPDATES.getValue() ) ) {
				mongodbCommander.update(execution, mongodbConfig.getUpdateQueries());
			} else if( mode.equals( Config.ExecutionMode.DELETES.getValue() ) ) {
				mongodbCommander.delete(execution, mongodbConfig.getDeleteQueries());
			} else {
				log.warn("Wrong execution.mode value: '{}' for execution.what='{}' in the configuration file.", mode, what);
			}
		} else {
			log.warn("Wrong execution.what value: '{}' in the configuration file.", what);
		}
	}
}

