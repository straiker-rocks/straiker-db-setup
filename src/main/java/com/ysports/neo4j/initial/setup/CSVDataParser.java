package com.ysports.neo4j.initial.setup;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;

public class CSVDataParser {
	
    public static Set<String[]> extractDataToMap(String route) throws IOException {
	   
		File file = new File(route);
	
		CSVParser parser = new CSVParser(new FileReader(file), CSVStrategy.EXCEL_STRATEGY);
		String[] lines = parser.getLine();
		
		Set<String[]> data = new HashSet<String[]>();
			while (lines != null) {
			   data.add(lines);
			   lines = parser.getLine();
			}
		return data;
    }
}
