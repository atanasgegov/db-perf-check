package com.akg.data.perf.comparisons.service;

import java.io.IOException;
import java.util.List;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.akg.data.perf.comparisons.config.ElasticsearchConfig;
import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.config.pojo.QueryParams;
import com.akg.data.perf.comparisons.dto.WineDTO;
import com.akg.data.perf.comparisons.util.JsonParser;
import com.akg.data.perf.comparisons.util.QueryUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ElasticsearchCommander extends AbstractCommander {

	private static final String ID_PRFIX = "\"_id\":\"";
	private static final String ID_SUFFIX = "\",";
	private static final String EXEC_HTTP_REQUEST_MSG = "Executing HTTP request to the endpoint {} with body {}";

	@Autowired
	private RestClient restClient;

	@Autowired
	private ElasticsearchConfig esConfig;

	@Override
	protected int insertRequest(List<WineDTO> data) {

		String endpoint = esConfig.getEndPoint().getInsert();
		String json = JsonParser.convertToESJson(data);
		Request request = new Request("POST", endpoint);
		request.setEntity(new NStringEntity(json, ContentType.APPLICATION_JSON));

		try {
			log.debug(EXEC_HTTP_REQUEST_MSG, endpoint, json);
			Response response = restClient.performRequest(request);
			log.debug("Done: {}", response.getStatusLine());
			return 1;
		} catch (IOException e) {
			log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING, endpoint, e.getMessage());
			return 0;
		}
	}

	@Override
	protected int searchRequest(Query query) {

		Response response = sendRequest( esConfig.getEndPoint().getSearch(), query.getExec(), query.getParams() ); 
		if (response != null && response.getStatusLine().getStatusCode() < 300) {
			return 1;
		}
		return 0;
	}

	@Override
	protected int deleteRequest(Query query) {

		Response response = sendRequest( esConfig.getEndPoint().getDelete(), query.getExec(), query.getParams() );
		if (response != null && response.getStatusLine().getStatusCode() < 300) {
			return 1;
		}
		return 0;
	}

	private Response sendRequest(String endpoint, String queryExec, QueryParams[] params) {

		Request request = new Request("POST", endpoint);
		try {
			String jsonEntity = QueryUtil.getQueryWithRandomChoosenParameter(queryExec, params);
			log.debug(EXEC_HTTP_REQUEST_MSG, endpoint, jsonEntity);
			request.setJsonEntity(jsonEntity);
			Response response = restClient.performRequest(request);
			log.debug("Done: {}.", response.getStatusLine());
			return response;
		} catch (IOException e) {
			log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING, endpoint, e.getMessage());
			return null;
		}
	}

	@Override
	protected int updateRequest(Query query) {

		int executed = 0;
		Response response = sendRequest(esConfig.getEndPoint().getSearch(), query.getAdditionalExec(),
				query.getParams());
		if (response != null && response.getStatusLine().getStatusCode() < 300) {

			List<String> ids;
			try {
				ids = QueryUtil.getIdsFromESResponse(response.getEntity().getContent(), ID_PRFIX, ID_SUFFIX);
				for (String id : ids) {
					executed = executed + 1;
					sendRequest(esConfig.getEndPoint().getUpdate() + id, query.getExec(), query.getParams());
				}
			} catch (UnsupportedOperationException | IOException e) {
				log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED, e.getMessage());
			}
		}

		return executed;
	}
}