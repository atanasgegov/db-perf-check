package com.akg.data.perf.comparisons.service;

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
import org.springframework.util.CollectionUtils;

import com.akg.data.perf.comparisons.WineMagDTO;
import com.akg.data.perf.comparisons.config.Config;
import com.akg.data.perf.comparisons.config.ElasticsearchConfig;
import com.akg.data.perf.comparisons.utils.JsonParser;

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

	public void search() {
		String endpoint = esConfig.getEndPoint().getSearch();
		Request request = new Request( "GET", endpoint );   
		try {
			Response response = restClient.performRequest(request);
			log.info( EntityUtils.toString(response.getEntity()) );
		} catch (IOException e) {
			log.error( "Something wrong happened calling {}, error: {}", endpoint, e.getMessage() );
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
}