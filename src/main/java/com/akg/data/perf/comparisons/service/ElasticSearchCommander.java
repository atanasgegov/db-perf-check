package com.akg.data.perf.comparisons.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.akg.data.perf.comparisons.WineMagDTO;
import com.akg.data.perf.comparisons.config.Config;
import com.akg.data.perf.comparisons.config.ElasticsearchConfig;
import com.akg.data.perf.comparisons.config.Query;
import com.akg.data.perf.comparisons.utils.JsonParser;
import com.akg.data.perf.comparisons.utils.QueryUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ElasticSearchCommander {

	@Autowired
	private RestClient restClient;

	@Autowired
	private ElasticsearchConfig esConfig;

	@Autowired
	private Config config;	
	
	@Autowired
	private WineMagLoader wineMagLoader;

	public int search() {

		Map<Integer, Query> queriesMap = calculatePercentageExecForQueries();
		long start = System.currentTimeMillis();
		long endTimeInMs = start + (config.getExecutionTimeInMs());

		int executed = 0;
		while (System.currentTimeMillis() <= endTimeInMs) {
			int index = ThreadLocalRandom.current().nextInt(0, 100);
			Query query = queriesMap.get(index);
			executed = executed + (search(query) ? 1 : 0);
		}

		log.info("The Elasticsearch predefined queries were executed {} times", executed);
		return executed;
	}

	public boolean search(Query query) {

		String endpoint = esConfig.getEndPoint().getSearch();
		Request request = new Request( "GET", endpoint );   
		try {
			String jsonEntity = QueryUtil.getQueryWithRandomChoosenParameter( query );
			request.setJsonEntity(jsonEntity);
			Response response = restClient.performRequest(request);
			log.info( EntityUtils.toString(response.getEntity()) );
			return true;
		} catch (IOException e) {
			log.error( "Something wrong happened calling {}, error: {}", endpoint, e.getMessage() );
			return false;
		}
	}

	public void insert() {

		int batchSize = config.getBatchSize();
		int batchSizeStart = 1; // skip the header 
		int batchSizeEnd = batchSize;
		for( int i=batchSizeStart;i<batchSizeEnd;i++ ) {
			List<WineMagDTO> data = null;
			try {
				data = wineMagLoader.load(batchSizeStart, batchSizeEnd);
			} catch (IOException e) {
				log.error( "Something wrong happened error: {}", e.getMessage() );
			}
			if( CollectionUtils.isEmpty(data) ) {
				break;
			}
			String json = JsonParser.convert(data);
			this.insertRequest(json);
			batchSizeStart = batchSizeEnd + 1;
			batchSizeEnd = batchSizeEnd + batchSize; 
		}
	}

	private void insertRequest( String body ) {

		String endpoint = esConfig.getEndPoint().getInsert();
		Request request = new Request( "POST", endpoint );
		request.setEntity(new NStringEntity( body, ContentType.APPLICATION_JSON));

		try {
			log.info( "Executing request to the endpoint {}", endpoint );
			Response response = restClient.performRequest(request);
			log.info( "Done. {}", response.getStatusLine() );
		} catch (IOException e) {
			log.error( "Something wrong happened calling {}, error: {}", endpoint, e.getMessage() );
		}
	}
	
	/**
	 * The method calculate a Map<Integer,Query>.
	 * The key will be a subsequent number value from 0 - 99. 
	 * The value will be a String or the actual query.
	 * If the queries are 5, then the result Map will be something like this:
	 *          {
	 * 				 0 -> {query1,...,...}
	 * 				 1 -> {query1,...,...}
	 *             ... 
	 *               n -> {query4,...,...}
	 *             ...
	 *              99 -> ,query5,...,...}
	 * 			}
	 * This Map later can be used with java Random class to be chosen random value from 0 to 100.
	 * @return  Map
	 */
	private Map<Integer, Query> calculatePercentageExecForQueries() {
		Map<Integer, Query> percentageQueryMap = new HashMap<>(100);

		List<Query> queries = esConfig.getQueries();
		Map<Integer, Query> upperNumberForPercentageCalcMap = calculateUpperNumberForPercentageCalc(
				queries);
		List<Integer> upperNumberPercentageValueList = new ArrayList<>(upperNumberForPercentageCalcMap.keySet());
		int count = 0;
		for (int i = 0; i < upperNumberForPercentageCalcMap.size(); i++) {
			for (Integer j = count; j < upperNumberPercentageValueList.get(i); j++) {
				percentageQueryMap.put(j, upperNumberForPercentageCalcMap.get(upperNumberPercentageValueList.get(i)));
			}
			count = upperNumberPercentageValueList.get(i);
		}

		return percentageQueryMap;
	}
	
	/**
	 * <p>
	 * Let the List<Query> has the following list of percentages [30, 40, 20, 5, 5] - the sum have to be 100. 
	 * Firstly the list should be sorted by percentage value - will become [5, 5, 20, 30, 40]. The goal of algorithm is to return upper number of a particular range:  
	 * Result will be:
	 *  from 0 to 5 -> 5 %
	 *       6-10 -> 5 %
	 *       11-30 -> 20 %
	 *       31-60 -> 30 %
	 *       61-100 -> 40%
	 *  or [5, 10, 30, 60, 100].
	 *  The returned Map will have for key the above described upper number value of a particular range and the value will be the actual Query object.
	 *  Then if we choose a number of 0 - 100 we will know which query will be chosen according to the key of the Map.
	 *  
	 *  Example of the returned Map:
	 *  { 
	 *    [5  -> {name: name1,data:...,percentage: 5}],
	 *    [10 -> {name: name2,data:...,percentage: 5}],
	 *	  [30 -> {name: name3,data:...,percentage: 20}], 
	 *	  ... 
	 *	} 
	 * </p>
	 * @param percentages
	 * @return
	 */
	private Map<Integer, Query> calculateUpperNumberForPercentageCalc(
			List<Query> queryList) {
		Collections.sort(queryList);
		Map<Integer, Query> upperNumberForPercentageCalc = new LinkedHashMap<>(queryList.size());
		Integer oldValue = 0;
		Integer newValue = 0;
		for (int i = 0; i < queryList.size(); i++) {
			newValue = oldValue + queryList.get(i).getPercentage();
			upperNumberForPercentageCalc.put(newValue, queryList.get(i));
			oldValue = newValue;
		}
		return upperNumberForPercentageCalc;
	}
}