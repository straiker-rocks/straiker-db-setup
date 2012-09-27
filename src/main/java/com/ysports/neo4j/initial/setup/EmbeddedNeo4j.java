package com.ysports.neo4j.initial.setup;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.FileUtils;


public class EmbeddedNeo4j {
	public enum INDEX {
		 USER,TEAM,LEAGUE,PLACE,CITY,COUNTRY,LOCALITY
	}
	
	private static final String DB_PATH = "/initialDB/neo4j-db";
	private static final String SPAIN_CITIES = "/initialDB/CSV/spaincities.csv";
	private static final String SPAIN_LOCALITIES = "/initialDB/CSV/spainlocalities.csv";
	private static final String SPAIN_PLACES = "/initialDB/CSV/spainplaces.csv";
	private static String NAME = "name";
	
	private static String[] COUNTRIES = {"Spain"};
    GraphDatabaseService graphDb;
    Node category;
    Node bowling;
    
    Index<Node> placeIndex;
    Index<Node> cityIndex;
	Index<Node> placesIndex;
	Index<Node> countryIndex;
    
    private static enum RelTypes implements RelationshipType
    {
      ROOT, SPORT, COUNTRY, CITY, LOCALITY, LOCATED, PLACE
    }
    // END SNIPPET: createReltype

    public static void main( final String[] args )
    {
        EmbeddedNeo4j hello = new EmbeddedNeo4j();
        hello.createDb();
        hello.removeData();
        hello.shutDown();
    }

	void createDb()
    {
        clearDb();
        graphDb = Neo4jTransactionManager.openGraphDb();
        
        registerShutdownHook( graphDb );
        Transaction tx = graphDb.beginTx();
        try
        {
        	IndexManager index = graphDb.index();
        	placeIndex= index.forNodes(INDEX.PLACE.name());
			cityIndex = index.forNodes(INDEX.CITY.name());
        	placesIndex = index.forNodes(INDEX.PLACE.name());
        	countryIndex = index.forNodes(INDEX.COUNTRY.name());
        	Node reference = graphDb.getReferenceNode();
        	category = graphDb.createNode();
        	reference.createRelationshipTo(category, RelTypes.ROOT);
        	category.setProperty(NAME, "Category" );
        	bowling = graphDb.createNode();
        	bowling.setProperty(NAME, "Bowling");
        	
        	category.createRelationshipTo(bowling, RelTypes.SPORT);
        	for (String country : COUNTRIES){				
        		Node pais = graphDb.createNode();
        		pais.setProperty(NAME, country );
        		category.createRelationshipTo(pais, RelTypes.COUNTRY);
        		//Indexamos todos los paises para agruparlos en places
        		placesIndex.add(pais, NAME, country.toLowerCase());
        		countryIndex.add(pais, NAME, country.toLowerCase());
        		if (country.equals("Spain")){
        			extractReducedSpainData(pais);
        		}
        	}

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
	
	private void extractReducedSpainData(Node places){
		try {
			Set<String[]> cities = CSVDataParser.extractDataToMap(SPAIN_CITIES);
			Set<String[]> localities = CSVDataParser.extractDataToMap(SPAIN_LOCALITIES);
			Set<String[]> playcenters = CSVDataParser.extractDataToMap(SPAIN_PLACES);
			Map<String, Long> storedCities = new HashMap<String, Long>(cities.size());
			Map<String, Long> storedLocalities = new HashMap<String, Long>(localities.size());
			for (String[] ciudades : cities){
				for (String [] center: playcenters){
					if (center[4].equals(ciudades[1])){
						Node ciudad = null;
						Long idNodo = storedCities.get(ciudades[1]);
						String cityKey = ciudades[0];
						if(idNodo == null){
							ciudad = graphDb.createNode();
							ciudad.setProperty(NAME, ciudades[1]);
							places.createRelationshipTo(ciudad, RelTypes.CITY);
							storedCities.put(ciudades[1], ciudad.getId());
							placeIndex.add(ciudad, NAME, ciudades[1]);
							cityIndex.add(ciudad, NAME, ciudades[1].toLowerCase());
						} else {
							ciudad = graphDb.getNodeById(idNodo);
						}
						
						for(String[] localidades : localities){
							if (cityKey.equals(localidades[0]) && center[3].equals(localidades[1])){
								storedLocalities = populateCityNodes(ciudad, localidades[1], center, storedLocalities);
		    				}
						}
					}
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	private void extractSpainData(Node places)  {
		try {
			Set<String[]> cities = CSVDataParser.extractDataToMap(SPAIN_CITIES);
			Set<String[]> localities = CSVDataParser.extractDataToMap(SPAIN_LOCALITIES);
			Set<String[]> playcenters = CSVDataParser.extractDataToMap(SPAIN_PLACES);
			for (String[] ciudades : cities){
				Node ciudad = graphDb.createNode();
				String cityKey = ciudades[0];
				ciudad.setProperty(NAME, ciudades[1]);
				places.createRelationshipTo(ciudad, RelTypes.CITY);
				for(String[] localidades : localities){
					if (cityKey.equals(localidades[0])){
    					populateCityNodes(ciudad, localidades[1], playcenters);
    				}
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		
	}
	
	private Map<String, Long> populateCityNodes(Node ciudad, String locality, String[] center, Map<String, Long> storedLocalities) {
		
		Node localidad = null;
		Long idNodo = storedLocalities.get(locality);
		if (idNodo == null){
			localidad = graphDb.createNode();
			localidad.setProperty(NAME, locality);
			ciudad.createRelationshipTo(localidad, RelTypes.LOCALITY);
			storedLocalities.put(locality, localidad.getId());
			
		} else {
			localidad = graphDb.getNodeById(idNodo);
		}
		populatePlacesNodes(localidad, center);
		return storedLocalities;
			
	}

	private void populateCityNodes(Node ciudad, String locality, Set<String[]> playcenters) {
		Node localidad = graphDb.createNode();
		localidad.setProperty(NAME, locality);
		ciudad.createRelationshipTo(localidad, RelTypes.CITY);
		for (String [] center: playcenters){
			if (center[3].equals(locality)){
				populatePlacesNodes(localidad, center);
			}
		}	
	}
	
	private void populatePlacesNodes(Node localidad, String[] lugar){
		Node place = graphDb.createNode();
		place.setProperty(NAME, lugar[0]);
		place.setProperty("address", lugar[1]);
		place.setProperty("pobox", lugar[2]);
		place.setProperty("phone", lugar[5]);
		place.setProperty("website", lugar[6]);
		//place.setProperty("email", lugar[7]);
		place.createRelationshipTo(localidad, RelTypes.LOCATED);
		place.createRelationshipTo(bowling, RelTypes.PLACE);
	}

    private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void removeData()
    {
    	ExecutionEngine engine = new ExecutionEngine( graphDb );
    	ExecutionResult result = engine.execute( "START n=node(*) MATCH n-[r?]-()WHERE ID(n) <> 0 return n, n.name" );
    	System.out.println( result );
    	
    	List<String> columns = result.columns();
        System.out.println( columns );
        String nodeResult;
        String rows = "";
        // END SNIPPET: columns
        // START SNIPPET: items
        Iterator<Node> n_column = result.columnAs( "n" );
        for ( Node node : IteratorUtil.asIterable( n_column ) )
        {
            // note: we're grabbing the name property from the node,
            // not from the n.name in this case.
            nodeResult = node + ": " + node.getProperty( "name" );
            System.out.println( nodeResult );
        }
        // END SNIPPET: items
        // the result is now empty, get a new one
        result = engine.execute( "START n=node(*) MATCH n-[r?]-()WHERE ID(n) <> 0 return n, n.name" );
        // START SNIPPET: rows
        for ( Map<String, Object> row : result )
        {
            for ( Entry<String, Object> column : row.entrySet() )
            {
                rows += column.getKey() + ": " + column.getValue() + "; ";
            }
            rows += "\n";
        }
        System.out.println( rows );
    }

    void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        Neo4jTransactionManager.closeGraphDb();
        // END SNIPPET: shutdownServer
    }

    // START SNIPPET: shutdownHook
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)graphDb.getReferenceNode()
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    // END SNIPPET: shutdownHook
}
