package com.akg.dbperfcheck.exec;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.akg.dbperfcheck.config.AbstractConfig;
import com.akg.dbperfcheck.config.CommonConfig;
import com.akg.dbperfcheck.config.pojo.Execution;
import com.akg.dbperfcheck.config.pojo.UseCases;
import com.akg.dbperfcheck.service.AbstractCommander;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class Executor {
	
	@Autowired
	private CommonConfig commonConfig;
	
	@Autowired 
	private BeanFactory beanFactory;
	
	@Autowired
	private ApplicationContext context;
	
	private ScheduledExecutorService shutdowner = Executors.newScheduledThreadPool(1);
	
	@EventListener(ApplicationReadyEvent.class)
	@Order(1)
	public void exec() {

	    long start = System.currentTimeMillis();
		if( commonConfig.getUseCases().getActiveUseCase().equals( UseCases.Type.ONE ) ) {

			this.run( commonConfig.getUseCases().getOne() );
		} else if( commonConfig.getUseCases().getActiveUseCase().equals( UseCases.Type.CRUD ) ) {

			for(Execution execution : commonConfig.getUseCases().getCrud()) {
				this.run(execution);
			}
		} else {
			log.warn("Please, check your configuration, probably the wrong value was set for active-use-case: {}", commonConfig.getUseCases().getActiveUseCase());		
		}

		long end = System.currentTimeMillis();
		log.info("Elapsed Time in seconds: "+ (double)(end-start)/1000);
		int shutdownAfter = 1;
		log.info("The application will shutdown in {} seconds.", shutdownAfter);
		this.shutdown(shutdownAfter);
	}

	private void run( Execution execution ) {

		String what = execution.getWhat();
		String mode = execution.getMode();
		AbstractConfig config = beanFactory.getBean( what+"Config", AbstractConfig.class );
		AbstractCommander commander = beanFactory.getBean(what+"Commander", AbstractCommander.class);

		log.info( "Executing '{}' '{}' queries for {} ms ...", what, mode, execution.getTimeInMs() );
		if( mode.equals( AbstractConfig.ExecutionMode.SEARCH.getValue() ) ) {
			commander.search(execution, config.getSearchQueries());
		} else if( mode.equals( AbstractConfig.ExecutionMode.INSERTS.getValue() ) ) {
			commander.insert(execution);
		} else if( mode.equals( AbstractConfig.ExecutionMode.UPDATES.getValue() ) ) {
			commander.update(execution, config.getUpdateQueries());
		} else if( mode.equals( AbstractConfig.ExecutionMode.DELETES.getValue() ) ) {
			commander.delete(execution, config.getDeleteQueries());
		} else {
			log.warn("Wrong execution.mode value: '{}' for execution.what='{}' in the configuration file.", mode, what);
		}
	}
	
	private void shutdown(int seconds) {
		Runnable shutdownerTask = () -> { 
			((ConfigurableApplicationContext) context).close();
			System.exit(0);
		};
		shutdowner.schedule(shutdownerTask, seconds, TimeUnit.SECONDS);
	}
}

