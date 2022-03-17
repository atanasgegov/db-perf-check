package com.akg.data.perf.comparisons.exec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.akg.data.perf.comparisons.config.Config;
import com.akg.data.perf.comparisons.config.Execution;
import com.akg.data.perf.comparisons.service.ElasticsearchCommander;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class Executor {
	
	@Autowired
	private Config config;
	
	@Autowired
	private ElasticsearchCommander elasticsearchCommander;
	
	@EventListener(ApplicationReadyEvent.class)
	@Order(1)
	public void exec() {

		long start = System.currentTimeMillis();
		if( config.getActiveUseCase().equals( Config.UseCase.ONE.getValue() ) ) {

			this.run( config.getUseCases().getOne() );
		} else if( config.getActiveUseCase().equals( Config.UseCase.CRUD.getValue() ) ) {

			for(Execution execution : config.getUseCases().getCrud()) {
				this.run(execution);
			}
		} else {
			log.warn("Please, check your configuration, probably the wrong value was set for active-use-case: {}", config.getActiveUseCase());		
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
				elasticsearchCommander.search(execution);
			} else if( mode.equals( Config.ExecutionMode.INSERTS.getValue() ) ) {
				elasticsearchCommander.insert(execution);
			} else if( mode.equals( Config.ExecutionMode.UPDATES.getValue() ) ) {
				elasticsearchCommander.update(execution);
			} else if( mode.equals( Config.ExecutionMode.DELETES.getValue() ) ) {
				elasticsearchCommander.delete(execution);
			} else {
				log.warn("Wrong execution.mode value: '{}' for execution.what='{}' in the configuration file.", mode, what);
			}
		} else {
			log.warn("Wrong execution.what value: '{}' in the configuration file.", what);
		}
	}
}

