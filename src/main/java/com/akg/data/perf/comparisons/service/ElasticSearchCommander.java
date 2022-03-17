package com.akg.data.perf.comparisons.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.akg.data.perf.comparisons.config.Config;
import com.akg.data.perf.comparisons.config.ElasticsearchConfig;
import com.akg.data.perf.comparisons.config.Execution;
import com.akg.data.perf.comparisons.config.Query;
import com.akg.data.perf.comparisons.config.QueryParams;
import com.akg.data.perf.comparisons.dto.WineMagDTO;
import com.akg.data.perf.comparisons.utils.JsonParser;
import com.akg.data.perf.comparisons.utils.QueryExecutionCounter;
import com.akg.data.perf.comparisons.utils.QueryUtil;
import com.akg.data.perf.comparisons.utils.WineMagLoader;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ElasticsearchCommander extends AbstractCommander {

	private static final String ID_PRFIX = "\"_id\":\"";
	private static final String ID_SUFFIX = "\",";
	private static final String ERROR_MSG_SOMETHING_WRONG_HAPPENED = "Something wrong happened error: {}";
	private static final String ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING = "Something wrong happened calling {}, error: {}";

	@Autowired
	private RestClient restClient;

	@Autowired
	private ElasticsearchConfig esConfig;

	@Autowired
	private Config config;

	public int search(Execution execution) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		Map<Integer, Query> queriesMap = QueryUtil.calculatePercentageExecForQueries(esConfig.getSearchQueries());
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, esConfig.getSearchQueries());
		queryExecutionCounter.printReccurently(config.getFrequencyOutputInMs());

		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int index = ThreadLocalRandom.current().nextInt(0, 100);
			Query query = queriesMap.get(index);
			Response response = doRequest(esConfig.getEndPoint().getSearch(), query.getExec(), query.getParams());
			if (response != null && response.getStatusLine().getStatusCode() < 300) {
				executed = executed + 1;
				queryExecutionCounter.increment(query);
			}
		}

		this.printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}

	public int insert(Execution execution) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		Query insertQuery = new Query("Batch Insert From JSON File");
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, Arrays.asList(insertQuery));
		queryExecutionCounter.printReccurently(config.getFrequencyOutputInMs());

		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int sizeOfRecordsForInsert = 0;
			try {
				sizeOfRecordsForInsert = WineMagLoader.getNumberOfRows(config.getInputDataFile());
			} catch (IOException e) {
				log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
			}
			int batchSize = config.getBatchSize();
			int batchSizeStart = 1; // skip the header
			int batchSizeEnd = ( batchSize > sizeOfRecordsForInsert ? sizeOfRecordsForInsert : batchSize );
			while (batchSizeStart <= sizeOfRecordsForInsert) {
				List<WineMagDTO> data = null;
				try {
					data = WineMagLoader.load(batchSizeStart, batchSizeEnd, config.getInputDataFile());
				} catch (IOException e) {
					log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
				}
				if (CollectionUtils.isEmpty(data) || System.currentTimeMillis() > endTimeInMs) {
					break;
				}
				String json = JsonParser.convert(data);
				this.doRequest(esConfig.getEndPoint().getInsert(), json);
				executed += (batchSizeEnd-batchSizeStart); 
				queryExecutionCounter.increment(insertQuery, (batchSizeEnd-batchSizeStart));
				batchSizeStart = batchSizeEnd + 1;
				batchSizeEnd = ( batchSizeEnd + batchSize ) > sizeOfRecordsForInsert ? sizeOfRecordsForInsert : ( batchSizeEnd + batchSize );
			}
		}

		this.printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}

	public int update(Execution execution) {

		Map<Integer, Query> queriesMap = QueryUtil.calculatePercentageExecForQueries(esConfig.getUpdateQueries());
		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, esConfig.getUpdateQueries());
		queryExecutionCounter.printReccurently(config.getFrequencyOutputInMs());

		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int index = ThreadLocalRandom.current().nextInt(0, 100);
			Query query = queriesMap.get(index);
			Response response = doRequest(esConfig.getEndPoint().getSearch(), query.getAdditionalExec(),
					query.getParams());
			if (response != null && response.getStatusLine().getStatusCode() < 300) {

				List<String> ids;
				try {
					ids = QueryUtil.getIdsFromESResponse(response.getEntity().getContent(), ID_PRFIX, ID_SUFFIX);
					for (String id : ids) {
						executed = executed + 1;
						queryExecutionCounter.increment(query);
						doRequest(esConfig.getEndPoint().getUpdate() + id, query.getExec(), query.getParams());
					}
				} catch (UnsupportedOperationException | IOException e) {
					log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
				}
			}
		}

		this.printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}

	public int delete(Execution execution) {

		long start = System.currentTimeMillis();
		long endTimeInMs = start + (execution.getTimeInMs());
		QueryExecutionCounter queryExecutionCounter = new QueryExecutionCounter(start, esConfig.getDeleteQueries());
		queryExecutionCounter.printReccurently(config.getFrequencyOutputInMs());

		int executed = 0;
		List<Query> deleteQueries = esConfig.getDeleteQueries();
		for (Query query : deleteQueries) {
			Response response = doRequest(esConfig.getEndPoint().getDelete(), query.getExec(), query.getParams());
			if (response != null && response.getStatusLine().getStatusCode() < 300) {
				executed = executed + 1;
				queryExecutionCounter.increment(query);
			}

			if (System.currentTimeMillis() > endTimeInMs) {
				break;
			}
		}

		this.printResult(execution, queryExecutionCounter);
		queryExecutionCounter.shutdownReccurentPrint();
		return executed;
	}

	private Response doRequest(String endpoint, String body) {

		Request request = new Request("POST", endpoint);
		request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));

		try {
			log.debug("Executing HTTP request to the endpoint {} with body {}", endpoint, body);
			Response response = restClient.performRequest(request);
			log.debug("Done: {}", response.getStatusLine());
			return response;
		} catch (IOException e) {
			log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING, endpoint, e.getMessage());
			return null;
		}
	}

	private Response doRequest(String endpoint, String queryData, QueryParams[] params) {

		Request request = new Request("POST", endpoint);
		try {
			String jsonEntity = QueryUtil.getQueryWithRandomChoosenParameter(queryData, params);
			log.debug("Executing HTTP request to the endpoint {} with body {}", endpoint, jsonEntity);
			request.setJsonEntity(jsonEntity);
			Response response = restClient.performRequest(request);
			log.debug("Done: {}.", response.getStatusLine());
			return response;
		} catch (IOException e) {
			log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING, endpoint, e.getMessage());
			return null;
		}
	}

	private void printResult(Execution execution, QueryExecutionCounter queryExecutionCounter) {

		String doneMessage = "Done" + System.lineSeparator() + execution.toString();
		String queryExecutionCounterMessage = System.lineSeparator() + queryExecutionCounter.toString();
		log.info( doneMessage + queryExecutionCounterMessage  );
	}
}