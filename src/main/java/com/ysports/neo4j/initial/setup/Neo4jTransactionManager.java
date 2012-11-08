package com.ysports.neo4j.initial.setup;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Neo4jTransactionManager {
	
	private static String NEOSTORE = "/initialDB/neo4j-db";
	private static final ThreadLocal<GraphDatabaseService> tl = new ThreadLocal<GraphDatabaseService>();
	
	public static GraphDatabaseService openGraphDb(){
		GraphDatabaseService graphDb = tl.get();
		if (graphDb == null){
			graphDb = new EmbeddedGraphDatabase(NEOSTORE);
			tl.set(graphDb);
		}
		return graphDb;
	}
	
	public static void closeGraphDb(){
		GraphDatabaseService graphDb = tl.get();
		tl.set(null);
		if (graphDb != null ) {
			graphDb.shutdown();
		}
	}
	
}
