package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.WebResource;

public class Neo4j_Graph_Store_test {

//	public static Set<Integer> VisitedVertices = new HashSet();
	public static HashSet<Long> VisitedVertices = new HashSet<Long>();
	
	public static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	public static void main(String[] args) 
	{
		OwnMethods.RestartNeo4jClearCache("Patents");
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		WebResource resource = p_neo.GetCypherResource();
		Neo4j_Graph_Store.Execute(resource, "match (n) where id(n) = 100 return n");
		long start = System.currentTimeMillis();
		//String query = "match (n)-[*]->(a) where id(n) = 4000002 return distinct a";
		String query = "match (n)-[*1..2]->(a) where id(n) = 4000000 return distinct a";
//		String query = "match (n) where id(n) = 4000001 return n";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		//Neo4j_Graph_Store.Graph_Traversal(resource, 4000014, VisitedVertices);
		long time = System.currentTimeMillis() - start;
		JsonArray arr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		System.out.println(time);
		//System.out.println(VisitedVertices.size());
		System.out.println(arr.size());
		
		OwnMethods.RestartNeo4jClearCache("Patents");
		p_neo = new Neo4j_Graph_Store();
		resource = p_neo.GetCypherResource();
		Neo4j_Graph_Store.Execute(resource, "match (n) where id(n) = 100 return n");
		start = System.currentTimeMillis();
		query = "match (n)-[*1..2]->(a) where id(n) = 4000000 return a";
//		query = "match (n)-->(a) where id(n) = 4000001 return distinct a";
		result = Neo4j_Graph_Store.Execute(resource, query);
		time = System.currentTimeMillis() - start;
		arr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		System.out.println(time);
		System.out.println(arr.size());
//		for(int j = 0;j<100;j++)
//		{
//			WebResource resource = p_neo.GetCypherResource();
//			for(int i = 0;i<100;i++)
//			{
//				String query = "match (n:Graph_Random_20) return id(n) limit " + i;
//				System.out.println(p_neo.Execute(resource, query));			
//			}
//		}
		
		
		/*JsonArray jsonArr = p_neo4j_graph_store.GetVertexIDandAllAttributes(3946862);
		System.out.println(jsonArr.toString());
		
		int id = jsonArr.get(0).getAsInt();
		System.out.println(id);
		
		JsonObject jsonObject = (JsonObject)jsonArr.get(1);
		System.out.println(jsonObject.toString());*/
		
		//String query = "match (a:Graph_Random_20) where a.id in [0,1,2,3,4,5] return id(a)";
//		String query = "match (a:Graph_Random_20) where a.id in [0] return a";
//		
//		JsonArray jsonArr = p_neo4j_graph_store.GetExecuteResultDataASJsonArray(p_neo4j_graph_store.Execute(query));
//		System.out.println(jsonArr.toString());
//		for(int j = 0;j<jsonArr.size();j++)
//		{
//			JsonObject jsonOb = (JsonObject) jsonArr.get(j);
//			JsonArray arr = jsonOb.get("row").getAsJsonArray();
//			long id = arr.get(0).getAsLong();
//			System.out.println(id);
//		}
//		if(jsonObject.has("RMBR_minx"))
//			System.out.println("has RMBR");
//		if(jsonObject.has("name"))
//			System.out.println("has layer");
//		if(jsonObject.has("longitude"))
//			System.out.println("has longitude");
	}

}
