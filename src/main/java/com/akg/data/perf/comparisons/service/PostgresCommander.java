package com.akg.data.perf.comparisons.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.akg.data.perf.comparisons.config.PostgresConfig;
import com.akg.data.perf.comparisons.config.pojo.Query;
import com.akg.data.perf.comparisons.config.pojo.QueryParams;
import com.akg.data.perf.comparisons.dto.WineDTO;
import com.akg.data.perf.comparisons.util.QueryUtil;

import lombok.extern.slf4j.Slf4j;

@Service("postgresCommander")
@Slf4j
public class PostgresCommander extends AbstractCommander {

	@Autowired(required=false)
	private PostgresConfig postgresConfig;
	
	@Autowired(required=false)
	private DataSource dataSource;

	private Connection conn;
	
	@PostConstruct
	protected void init() {
		try {
			conn = dataSource.getConnection();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void closeResources() {
		if (conn != null) {
			try {
				conn.close();
				log.info(INFO_MSG_RESOURCES_CLOSED);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	protected Integer insertRequest(List<WineDTO> data) {

		try (PreparedStatement preparedStatement = conn.prepareStatement(
					"insert into wines.wine (id,num,country,description,designation,points,price,province,region1,region2,variety,winery) " +
					  "values (?,?,?,?,?,?,?,?,?,?,?,?)");){
			
			for (WineDTO wine : data) {
				preparedStatement.setLong(1, wine.getId());
				preparedStatement.setInt(2, wine.getNum());
				preparedStatement.setString(3, wine.getCountry());
				preparedStatement.setString(4, wine.getDescription());
				preparedStatement.setString(5, wine.getDesignation());
				preparedStatement.setInt(6, wine.getPoints());
				preparedStatement.setFloat(7, wine.getPrice());
				preparedStatement.setString(8, wine.getProvince());
				preparedStatement.setString(9, wine.getRegion1());
				preparedStatement.setString(10, wine.getRegion2());
				preparedStatement.setString(11, wine.getVariety());
				preparedStatement.setString(12, wine.getWinery());
				preparedStatement.addBatch();
			}
	
	        return preparedStatement.executeBatch().length;
		} catch( SQLException e ) {
			log.error(e.getMessage());
			return 0;
		}
	}

	@Override
	protected Integer searchRequest(Query query) {

		this.executeQuery(query.getExec(),query.getParams());
		return 1;
	}

	@Override
	protected Integer updateRequest(Query query) {
		return this.executeUpdateOrDelete(query.getExec(), query.getParams());
	}

	@Override
	protected Integer deleteRequest(Query query) {
		return this.executeUpdateOrDelete(query.getExec(), query.getParams());
	}

	@Override
	protected Long getMaxId() {
		try( PreparedStatement ps = conn.prepareStatement(postgresConfig.getMaxIdQuery()) ) {
			ResultSet rs = ps.executeQuery();
			Long maxId = null;
			while(rs.next()) {
				maxId = rs.getLong(1);
				break;
			}
			return maxId;
		} catch(SQLException e) {
			log.error(e.getMessage());
			return null;
		}
	}

	private ResultSet executeQuery(String exec, QueryParams[] params) {

		String sql = QueryUtil.getQueryWithRandomChoosenParameter(exec, params);
		try( PreparedStatement ps = conn.prepareStatement(sql) ) {
			return ps.executeQuery();
		} catch(SQLException e) {
			log.error(e.getMessage());
			return null;
		}
	}

	private int executeUpdateOrDelete(String exec, QueryParams[] params) {

		String sql = QueryUtil.getQueryWithRandomChoosenParameter(exec, params);
		try( PreparedStatement ps = conn.prepareStatement(sql) ) {
			return ps.executeUpdate();
		} catch(SQLException e) {
			log.error(e.getMessage());
			return 0;
		}
	}
}