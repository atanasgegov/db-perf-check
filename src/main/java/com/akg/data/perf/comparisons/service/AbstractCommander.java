package com.akg.data.perf.comparisons.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.akg.data.perf.comparisons.config.CommonConfig;
import com.akg.data.perf.comparisons.config.pojo.Execution;
import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.dto.WineDTO;
import com.akg.data.perf.comparisons.util.QueryExecutionCounter;
import com.akg.data.perf.comparisons.util.QueryUtil;
import com.akg.data.perf.comparisons.util.WinesDataLoader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCommander {
	
	protected static final String ERROR_MSG_SOMETHING_WRONG_HAPPENED = "Something wrong happened error: {}";
	protected static final String ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING = "Something wrong happened calling {}, error: {}";
	
	@Autowired
	protected CommonConfig commonConfig;
	
	protected abstract Integer insertRequest(List<WineDTO> data);
	protected abstract Integer searchRequest(Query query);
	protected abstract Integer deleteRequest(Query query);
	protected abstract Integer updateRequest(Query query);
	protected abstract Long getMaxId();

	public int search(Execution execution, List<Query> queries) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		Map<Integer, Query> queriesMap = QueryUtil.calculatePercentageExecForQueries( queries);
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, queries);
		queryExecutionCounter.printReccurently(commonConfig.getFrequencyOutputInMs());

		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int index = ThreadLocalRandom.current().nextInt(0, 100);
			Query query = queriesMap.get(index);
			Integer isSuccessful = searchRequest(query);
			if (isSuccessful > 0) {
				executed = executed + 1;
				queryExecutionCounter.increment(query);
			}
		}

		printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}
	
	public int insert(Execution execution) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		Query insertQuery = new Query("Batch Insert From CSV File");
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, Arrays.asList(insertQuery));
		queryExecutionCounter.printReccurently(commonConfig.getFrequencyOutputInMs());

		long maxId = this.getMaxId(); 
		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int sizeOfRecordsForInsert = 0;
			try {
				sizeOfRecordsForInsert = WinesDataLoader.getNumberOfRows(commonConfig.getInputDataFile());
			} catch (IOException e) {
				log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
			}
			int batchSize = commonConfig.getBatchSize();
			int batchSizeStart = 1; // skip the header
			int batchSizeEnd = ( batchSize > sizeOfRecordsForInsert ? sizeOfRecordsForInsert : batchSize );
			while (batchSizeStart <= sizeOfRecordsForInsert) {
				List<WineDTO> data = null;
				try {
					data = WinesDataLoader.load(batchSizeStart, batchSizeEnd, commonConfig.getInputDataFile(), maxId);
				} catch (IOException e) {
					log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
				}
				if (CollectionUtils.isEmpty(data) || System.currentTimeMillis() > endTimeInMs) {
					break;
				}
				insertRequest( data );
				executed += (batchSizeEnd-batchSizeStart); 
				queryExecutionCounter.increment(insertQuery, (batchSizeEnd-batchSizeStart));
				batchSizeStart = batchSizeEnd + 1;
				batchSizeEnd = ( batchSizeEnd + batchSize ) > sizeOfRecordsForInsert ? sizeOfRecordsForInsert : ( batchSizeEnd + batchSize );
				maxId = maxId + data.size();
			}
		}

		this.printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();

		return executed;
	}

	public int update(Execution execution, List<Query> updateQueries) {

		Map<Integer, Query> queriesMap = QueryUtil.calculatePercentageExecForQueries(updateQueries);
		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, updateQueries);
		queryExecutionCounter.printReccurently(commonConfig.getFrequencyOutputInMs());

		Integer executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int index = ThreadLocalRandom.current().nextInt(0, 100);
			Query query = queriesMap.get(index);
			executed = updateRequest(query);
			queryExecutionCounter.increment(query,executed);
		}

		printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}	

	public int delete(Execution execution, List<Query> deleteQueries) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, deleteQueries);
		queryExecutionCounter.printReccurently(commonConfig.getFrequencyOutputInMs());

		Integer executed = 0;
		for (Query query : deleteQueries) {
			executed = deleteRequest(query);
			if (executed>0) {
				queryExecutionCounter.increment(query,executed);
			}

			if (System.currentTimeMillis() > endTimeInMs) {
				break;
			}
		}

		printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}
	
	protected void printResult(Execution execution, QueryExecutionCounter queryExecutionCounter) {

		String doneMessage = "Done" + System.lineSeparator() + execution.toString();
		String queryExecutionCounterMessage = System.lineSeparator() + queryExecutionCounter.toString();
		log.info( doneMessage + queryExecutionCounterMessage  );
	}

}
