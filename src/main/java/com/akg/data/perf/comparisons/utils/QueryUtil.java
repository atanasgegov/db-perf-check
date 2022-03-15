package com.akg.data.perf.comparisons.utils;

import java.util.concurrent.ThreadLocalRandom;

import com.akg.data.perf.comparisons.config.Query;
import com.akg.data.perf.comparisons.config.QueryParams;

import lombok.experimental.UtilityClass;

@UtilityClass
public class QueryUtil {
	
	public static String getQueryWithRandomChoosenParameter(Query query) {
		String queryData = query.getData();
		int paramSize = query.getParams().length;
		if( paramSize > 0 ) {
			int index = ThreadLocalRandom.current().nextInt(0, paramSize);
			QueryParams qp = query.getParams()[index];
			queryData = queryData.replace( "?1", qp.getParam1());
			queryData = queryData.replace( "?2", qp.getParam2());
		}

		return queryData;
	}
}
