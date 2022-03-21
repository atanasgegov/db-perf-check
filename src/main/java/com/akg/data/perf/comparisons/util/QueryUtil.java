package com.akg.data.perf.comparisons.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;

import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.config.pojo.QueryParams;

import lombok.experimental.UtilityClass;

@UtilityClass
public class QueryUtil {
	
	public static String getQueryWithRandomChoosenParameter(String queryData, QueryParams[] params) {
		int paramSize = params.length;
		if( paramSize > 0 ) {
			int index = ThreadLocalRandom.current().nextInt(0, paramSize);
			QueryParams qp = params[index];
			queryData = queryData.replace( "?1", qp.getParam1());
			queryData = queryData.replace( "?2", qp.getParam2());
		}

		return queryData;
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
	public static Map<Integer, Query> calculatePercentageExecForQueries(List<Query> queries) {
		Map<Integer, Query> percentageQueryMap = new HashMap<>(100);

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
	
	public List<String> getIdsFromESResponse(InputStream is, String idPrefix, String idSuffix) throws IOException {
		StringWriter writer = new StringWriter();
		IOUtils.copy(is, writer, "UTF-8");
		String responseData = writer.toString();
		
		return getIdsFromESResponse( responseData, idPrefix, idSuffix );
	}
	
	/**
	 *   Return List of id taken from String. For example:
	 * blabla id: 123, blabla id: 124, blabla. Then result will be a list=[123,124].
	 * In this case idPrefix = "id: " and idSuffix = ",". 
	 * @param str
	 * @param idPrefix
	 * @param idSuffix
	 * @return
	 */
	private List<String> getIdsFromESResponse( String str, String idPrefix, String idSuffix ) {
		List<String> listOfIds = new ArrayList<>();
		int fromIndex = 0;
		int endIndex = 0;

		while (fromIndex != -1) {

		    fromIndex = str.indexOf(idPrefix, fromIndex);
		    
		    if (fromIndex != -1) {
			    endIndex = str.indexOf(idSuffix, fromIndex);
		    	fromIndex += idPrefix.length();
			    String id = str.substring( fromIndex, endIndex );
		    	listOfIds.add( id );
		    }
		}

		return listOfIds;
	}
}
