package com.akg.dbperfcheck.service;

import java.util.List;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.akg.dbperfcheck.config.MongodbConfig;
import com.akg.dbperfcheck.config.pojo.Query;
import com.akg.dbperfcheck.dto.WineDTO;
import com.akg.dbperfcheck.util.JsonParser;
import com.akg.dbperfcheck.util.QueryUtil;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.WriteModel;

import lombok.extern.slf4j.Slf4j;

@Service("mongodbCommand")
@Slf4j
public class MongodbCommand extends AbstractCommand {

	@Autowired(required=false)
	private MongodbConfig mongodbConfig;

	@Autowired(required=false)
	private MongoClient mongoClient;

	private MongoDatabase db;

	@PostConstruct
	public void init() {
		if( mongoClient != null ) {
			db = mongoClient.getDatabase(mongodbConfig.getDatabase());
		}
	}

	protected Integer insertRequest(List<WineDTO> data) {
		try {
			List<WriteModel<Document>> writeModelList = JsonParser.convertToListOfMongodbModel(data);
			MongoCollection<Document> collection = db.getCollection(mongodbConfig.getCollection());
			BulkWriteResult result = collection.bulkWrite(writeModelList);
			if( result.getInsertedCount() > 0 ) {
				return 1;
			}

			return 0;
        } catch (MongoException me) {
            log.error("The bulk write operation failed due to an error: " + me);
    		return 0;
        }
	}

	@Override
	protected Integer searchRequest(Query query) {

		String exec = QueryUtil.getQueryWithRandomChoosenParameter(query.getExec(), query.getParams());
		Bson bsonCmd = Document.parse(exec);

		// Execute the native query
		Document result = db.runCommand(bsonCmd);

		// Get the output
		Document cursor = (Document) result.get("cursor");
		List<Document> docs = (List<Document>) cursor.get("firstBatch");
		//docs.forEach(d->log.info(d.toString()));
		
		return 1;
	}

	@Override
	protected Integer deleteRequest(Query query) {
		return this.deleteOrUpdateRequest(query);
	}

	@Override
	protected Integer updateRequest(Query query) {
		return this.deleteOrUpdateRequest(query);
	}
	
	@Override
	protected Long getMaxId() {
		Bson bsonCmd = Document.parse(mongodbConfig.getMaxIdQuery().getExec());

		// Execute the native query
		Document result = db.runCommand(bsonCmd);
		Document cursor = (Document) result.get("cursor");
		List<Document> docs = (List<Document>) cursor.get("firstBatch");
		Document maxId = docs.get(0);
		
		return Long.valueOf( maxId.getInteger("max") );
	}

	@Override
	public void closeResources() {
		if( mongoClient != null ) {
			mongoClient.close();
			log.info(INFO_MSG_RESOURCES_CLOSED);
		}
	}

	private int deleteOrUpdateRequest(Query query) {
		String exec = QueryUtil.getQueryWithRandomChoosenParameter(query.getExec(), query.getParams());
		Bson bsonCmd = Document.parse(exec);

		// Execute the native query
		Document result = db.runCommand(bsonCmd);

		return (Integer)result.get("n");
	}
}