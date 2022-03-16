package com.akg.data.perf.comparisons.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.akg.data.perf.comparisons.dto.WineMagDTO;

import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WineMagLoader {

	public static List<WineMagDTO> load(int startIndex, int endIndex, String inputDataFile ) throws IOException {
		List<WineMagDTO> list = new ArrayList<>();
		try( Reader in = new FileReader(inputDataFile) ) {
			Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

			int counter = 0;
			for (CSVRecord csvRecord : records) {
				if( counter++ < startIndex ) {
					continue;
				}
				if( counter > endIndex ) {
					break;
				}

				WineMagDTO wmDTO = new WineMagDTO();
				wmDTO.setId(Integer.valueOf(csvRecord.get(0)));
				wmDTO.setCountry(csvRecord.get(1));
				wmDTO.setDescription(csvRecord.get(2));
				wmDTO.setDesignation(csvRecord.get(3));
				wmDTO.setPoints( StringUtil.isNullOrEmpty( csvRecord.get(4) ) ? -1 : Integer.valueOf(csvRecord.get(4)) );
				wmDTO.setPrice( StringUtil.isNullOrEmpty( csvRecord.get(5) ) ? -1 : Float.valueOf(csvRecord.get(5)) );
				wmDTO.setProvince(csvRecord.get(6));
				wmDTO.setRegion1(csvRecord.get(7));
				wmDTO.setRegion2(csvRecord.get(8));
				wmDTO.setVariety(csvRecord.get(9));
				wmDTO.setWinery(csvRecord.get(10));
				list.add(wmDTO);
			}
			log.debug( "The current CSV file offset is {}", counter );
		}
		
		return list;
	}

	public static int getNumberOfRows(String inputDataFile ) throws IOException {
		
		int counter = 0;
		try(BufferedReader br = new BufferedReader(new FileReader(inputDataFile))) {  
			while (br.readLine() != null) {
				counter++;
			}
		}

		return counter;
	}
}