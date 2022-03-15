package com.akg.data.perf.comparisons.utils;

import java.util.List;

import com.akg.data.perf.comparisons.dto.WineMagDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonParser {

	private static final String JSON_INDEX = "{ \"index\": {} }";
	
	public static String convert( List<WineMagDTO> winesRecords ) {
		
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
}
