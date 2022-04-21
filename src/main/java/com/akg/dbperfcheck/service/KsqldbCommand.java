package com.akg.dbperfcheck.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.akg.dbperfcheck.config.KsqldbConfig;
import com.akg.dbperfcheck.config.pojo.Query;
import com.akg.dbperfcheck.dto.WineDTO;
import com.akg.dbperfcheck.util.QueryUtil;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import lombok.extern.slf4j.Slf4j;

@Service("ksqldbCommand")
@Slf4j
public class KsqldbCommand extends AbstractCommand {

	@Autowired(required=false)
	private Client client;

	@Autowired(required=false)
	private KsqldbConfig ksqldbConfig;

	private static final Map<String, Object> KSLQDB_PROPERTIES = Collections.singletonMap(
			  "auto.offset.reset", "earliest"
			);
	
	@Override
	protected Integer insertRequest(List<WineDTO> data) {
		List<KsqlObject> rows = new ArrayList<>(data.size());
		for (WineDTO wine : data) {
			KsqlObject row = new KsqlObject()
				    .put("id", wine.getId())
				    .put("num", wine.getNum())
				    .put("country", wine.getCountry())
				    .put("description", wine.getDescription() )
				    .put("designation", wine.getDesignation() )
				    .put("points", wine.getPoints())
				    .put("price", wine.getPrice())
				    .put("province", wine.getProvince())
				    .put("region1", wine.getRegion1())
				    .put("region2", wine.getRegion2())
				    .put("variety", wine.getVariety())
				    .put("winery", wine.getWinery());
			rows.add(row);
		}

		CompletableFuture<Void> result = CompletableFuture.allOf(
				  rows.stream().map(row -> client.insertInto("s_wines", row)).toArray(CompletableFuture[]::new)
				);

		result.thenRun(() -> log.info("Seeded transaction events."));

		result.join();
		
        return rows.size();
	}

	@Override
	protected Integer searchRequest(Query query) {
		String queryExec = QueryUtil.getQueryWithRandomChoosenParameter(query.getExec(), query.getParams());
		BatchedQueryResult batchedQueryResult = client.executeQuery( queryExec, KSLQDB_PROPERTIES );
		try {
			List<Row> rows = batchedQueryResult.get(1,TimeUnit.SECONDS);
			log.debug("Done: {}.", rows.toArray());
			return 1;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			Thread.currentThread().interrupt();
		}

		return 0;
	}

	@Override
	protected Integer updateRequest(Query query) {

		int executed = 0;

		return executed;
	}

	@Override
	protected Integer deleteRequest(Query query) {

		return 0;
	}

	@Override
	protected Long getMaxId() {
		Long maxId = 0L;
		try {
			CompletableFuture<?> createCountTable = client.executeStatement( ksqldbConfig.getMaxIdQuery().getAdditionalExec(),KSLQDB_PROPERTIES  );
			createCountTable.join();
			List<Row> queries = client.executeQuery( ksqldbConfig.getMaxIdQuery().getExec() ).get(1, TimeUnit.SECONDS);
			if(!CollectionUtils.isEmpty(queries)) {
				maxId = queries.get(0).getLong(1);
			}
		} catch (TimeoutException e) {
			log.warn( e.getMessage() );
			maxId = 0L;
		} catch (Exception e) {
			log.error( e.getMessage() );
			return null;
		}

		return maxId;
	}

	@Override
	public void closeResources() {
		if( client != null ) {
			try {
				CompletableFuture<?> cleanFuture = client.executeStatement( ksqldbConfig.getCleanQuery().getExec(), KSLQDB_PROPERTIES );
				cleanFuture.thenRun(() -> log.info("Closing KSQLDB resources."));
				cleanFuture.join();
				client.close();
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			log.info(INFO_MSG_RESOURCES_CLOSED);
		}
	}

}