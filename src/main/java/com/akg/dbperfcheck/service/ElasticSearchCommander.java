package com.akg.dbperfcheck.service;

import java.io.IOException;
import java.util.List;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.akg.dbperfcheck.config.ElasticsearchConfig;
import com.akg.dbperfcheck.config.pojo.Query;
import com.akg.dbperfcheck.config.pojo.QueryParams;
import com.akg.dbperfcheck.dto.WineDTO;
import com.akg.dbperfcheck.util.JsonParser;
import com.akg.dbperfcheck.util.QueryUtil;

import lombok.extern.slf4j.Slf4j;

@Service("elasticsearchCommander")
@Slf4j
public class ElasticsearchCommander extends AbstractCommander {

	private static final String ID_PRFIX = "\"_id\":\"";
	private static final String ID_SUFFIX = "\",";
	private static final String EXEC_HTTP_REQUEST_MSG = "Executing HTTP request to the endpoint {} with body {}";

	@Autowired(required=false)
	private RestClient restClient;

	@Autowired(required=false)
	private ElasticsearchConfig esConfig;

	@Override
	protected Integer insertRequest(List<WineDTO> data) {

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
	protected Integer searchRequest(Query query) {

		Response response = sendRequest( esConfig.getEndPoint().getSearch(), query.getExec(), query.getParams() ); 
		if (response != null && response.getStatusLine().getStatusCode() < 300) {
			return 1;
		}
		return 0;
	}

	@Override
	protected Integer updateRequest(Query query) {

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

	@Override
	protected Integer deleteRequest(Query query) {

		Response response = sendRequest( esConfig.getEndPoint().getDelete(), query.getExec(), query.getParams() );
		if (response != null && response.getStatusLine().getStatusCode() < 300) {
			return 1;
		}
		return 0;
	}

	@Override
	protected Long getMaxId() {
		try {
			Request request = new Request("POST", esConfig.getEndPoint().getSearch());
			request.setJsonEntity( esConfig.getMaxIdQuery() );
			Response response = restClient.performRequest(request);
			if (response != null && response.getStatusLine().getStatusCode() < 300) {
				String responseBody = EntityUtils.toString(response.getEntity());
				String value = JsonParser.getValueFromJsonDocument(responseBody, "/aggregations/max_id/value");
				
				return value!=null?Double.valueOf(value).longValue():null;
			}
			return 0L;
		} catch (IOException e) {
			log.error(ERROR_MSG_SOMETHING_WRONG_HAPPENED_CALLING, esConfig.getEndPoint().getSearch(), e.getMessage());
			return null;
		}
	}

	@Override
	public void closeResources() {
		if( restClient != null ) {
			try {
				restClient.close();
			} catch (IOException e) {
				log.error(e.getMessage());
			}
			log.info(INFO_MSG_RESOURCES_CLOSED);
		}
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
}