package com.akg.data.perf.comparisons.config;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.akg.data.perf.comparisons.config.pojo.EndPoint;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Configuration("elasticsearchConfig")
@ConfigurationProperties(prefix = "elasticsearch")
@Getter
@Setter
@Slf4j
public class ElasticsearchConfig extends AbstractConfig {

	private String protocol;
	private int socketTimeout;
	private int connectionTimeout;
	private EndPoint endPoint;
	private int ioThreadCount;

	@Bean
	public RestClient getEsRestClient() {
		
		try {
			RestClientBuilder builder = RestClient.builder(
			        new HttpHost(host, port, protocol));
			builder.setFailureListener(new RestClient.FailureListener() {
			    @Override
			    public void onFailure(Node node) {
			        log.error( "The node has been failured {}", node );
			    }
			});
			builder.setRequestConfigCallback(t -> t.setSocketTimeout(socketTimeout) );
			builder.setRequestConfigCallback(t -> t.setConnectTimeout(connectionTimeout));
			builder.setHttpClientConfigCallback(t -> t.setDefaultIOReactorConfig(
										                IOReactorConfig.custom()
										                    .setIoThreadCount(ioThreadCount)
										                    .build()));
	
			return builder.build();
		} catch( Exception e ) {
			log.warn(e.getMessage());
			return null;
		}
	}
	
	
}