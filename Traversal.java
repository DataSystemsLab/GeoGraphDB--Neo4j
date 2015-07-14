package def;

import java.util.*;
import java.io.*;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Traversal implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	private String longitude_property_name;
	private String latitude_property_name;
	
	
	public Traversal()
	{
		Config config = new Config();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
	}
	
	public void Preprocess()
	{
		
	}
	
	public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		Queue<Integer> queue = new LinkedList();
		VisitedVertices.clear();
		ArrayList<Integer> outneighbors = p_neo4j_graph_store.GetOutNeighbors(start_id);
		
		for(int i = 0;i<outneighbors.size();i++)
		{
			queue.add(outneighbors.get(i));
		}
		
		while(!queue.isEmpty())
		{
			int id = queue.poll();
			if(p_neo4j_graph_store.IsSpatial(id))
			{
				double lat = Double.parseDouble(p_neo4j_graph_store.GetVertexAttributeValue(id, latitude_property_name));
				double lon = Double.parseDouble(p_neo4j_graph_store.GetVertexAttributeValue(id, longitude_property_name));
				if(p_neo4j_graph_store.Location_In_Rect(lat, lon, rect))
				{
					System.out.println(id);
					return true;
				}
			}
			VisitedVertices.add(id);
			
			outneighbors = p_neo4j_graph_store.GetOutNeighbors(id);
			for(int i = 0;i<outneighbors.size();i++)
			{
				int outneighbor = outneighbors.get(i);
				if(!VisitedVertices.contains(outneighbor))
				{
					queue.add(outneighbor);
				}
			}
		}		
		return false;
	}

}