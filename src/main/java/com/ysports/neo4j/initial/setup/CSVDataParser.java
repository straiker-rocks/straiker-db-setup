package com.ysports.neo4j.initial.setup;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;

public class CSVDataParser {
	
    public static List<String[]> extractDataToMap(String route,boolean omitFirstRow) throws IOException {
	   
		File file = new File(route);
	
		CSVParser parser = new CSVParser(new FileReader(file), CSVStrategy.EXCEL_STRATEGY);
		String[] lines = parser.getLine();
		
		if(omitFirstRow)
			lines = parser.getLine();
		List<String[]> data = new ArrayList<String[]>();
			while (lines != null) {
			   data.add(lines);
			   lines = parser.getLine();
			}
		return data;
    }
}
