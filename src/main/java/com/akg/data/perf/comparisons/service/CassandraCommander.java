package com.akg.data.perf.comparisons.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.akg.data.perf.comparisons.config.CassandraConfig;
import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.config.pojo.QueryParams;
import com.akg.data.perf.comparisons.dto.WineDTO;
import com.akg.data.perf.comparisons.util.QueryUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import lombok.extern.slf4j.Slf4j;

@Service("cassandraCommander")
@Slf4j
public class CassandraCommander extends AbstractCommander {

	@Autowired(required=false)
	private CassandraConfig cassandraConfig;
	
	@Autowired(required=false)
	private CqlSession cqlSession;

	@Override
	protected Integer insertRequest(List<WineDTO> data) {

		PreparedStatement prepared = cqlSession.prepare(
				  "insert into wines.wine (id,num,country,description,designation,points,price,province,region1,region2,variety,winery) " +
				  "values (?,?,?,?,?,?,?,?,?,?,?,?)");
		for (WineDTO wine : data) {
			BoundStatement bound = prepared.bind(wine.getId(),wine.getNum(),wine.getCountry(),wine.getDescription(),wine.getDesignation(),wine.getPoints(),wine.getPrice(),wine.getProvince(),wine.getRegion1(),wine.getRegion2(),wine.getVariety(),wine.getWinery());
			cqlSession.execute(bound);
		}

		return 1;
	}

	@Override
	protected Integer searchRequest(Query query) {

		this.executeQuery(query.getExec(),query.getParams());
		return 1;
	}

	@Override
	protected Integer deleteRequest(Query query) {
		return this.updateOrDeleteRequest(query);
	}

	@Override
	protected Integer updateRequest(Query query) {
		return this.updateOrDeleteRequest(query);
	}
	
	private Integer updateOrDeleteRequest(Query query) {
		ResultSet rs = this.executeQuery(query.getAdditionalExec(),query.getParams());
		List<Row> rows = rs.all();
		PreparedStatement prepared = cqlSession.prepare(query.getExec());
		for (Row row : rows) {
			cqlSession.execute(prepared.bind(row.getLong("ID")));
		}
		return rows.size();
	}

	private ResultSet executeQuery(String exec, QueryParams[] params) {

		String cql = QueryUtil.getQueryWithRandomChoosenParameter(exec, params);
		return cqlSession.execute( cql );
	}
	
	@Override
	protected Long getMaxId() {

		ResultSet rs = cqlSession.execute(cassandraConfig.getMaxIdQuery());
		Row row = rs.one();
		Long maxId = 0L;
		try {
			maxId = row.getLong("id");
		} catch( Exception e ) {
			log.error( e.getMessage() );
		}

		return maxId;
	}
}
