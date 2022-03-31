package com.akg.dbperfcheck.util;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.akg.dbperfcheck.dto.WineDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonParser {

	private static final String JSON_INDEX = "{ \"index\": {} }";
	
	public static String convertToESJson( List<WineDTO> winesRecords ) {
		
		StringBuilder json = new StringBuilder();
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		winesRecords.stream().forEach( c -> {
			try {
				json.append( JSON_INDEX );
				json.append( System.lineSeparator() );
				json.append( ow.writeValueAsString(c).replaceAll( System.lineSeparator(), "") );
				json.append( System.lineSeparator() );
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});
		
		return json.toString();
	}
	
	public static List<WriteModel<Document>> convertToListOfMongodbModel( List<WineDTO> winesRecords ) {
		
		List<WriteModel<Document>> documents = new ArrayList<>();
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		winesRecords.stream().forEach( c -> {
			try {
				Document document = Document.parse(ow.writeValueAsString(c));
				documents.add(new InsertOneModel<>(document));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

		return documents;
	}
	
	public static String getValueFromJsonDocument( String json, String jsonPointerExpression ) {
		ObjectReader reader = new ObjectMapper().reader();
		JsonNode root;
		try {
			root = reader.readTree(json);
			return root.at(jsonPointerExpression).asText();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}
}
