package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Neo4j_Graph_Store_test {

	public static Set<Integer> VisitedVertices = new HashSet();
	
	public static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	public static void main(String[] args) {
		/*JsonArray jsonArr = p_neo4j_graph_store.GetVertexIDandAllAttributes(3946862);
		System.out.println(jsonArr.toString());
		
		int id = jsonArr.get(0).getAsInt();
		System.out.println(id);
		
		JsonObject jsonObject = (JsonObject)jsonArr.get(1);
		System.out.println(jsonObject.toString());*/
		
		String query = "match (a:Graph_Random_20) where a.id in [0,1,2,3,4,5] return a";
		
		JsonArray jsonArr = p_neo4j_graph_store.GetExecuteResultDataASJsonArray(p_neo4j_graph_store.Execute(query));
		for(int j = 0;j<jsonArr.size();j++)
		{
			JsonObject jsonOb = (JsonObject) jsonArr.get(j);
			JsonArray arr = jsonOb.get("row").getAsJsonArray();
			jsonOb = arr.get(0).getAsJsonObject();
			System.out.println(jsonOb.toString());
		}
//		if(jsonObject.has("RMBR_minx"))
//			System.out.println("has RMBR");
//		if(jsonObject.has("name"))
//			System.out.println("has layer");
//		if(jsonObject.has("longitude"))
//			System.out.println("has longitude");
	}

}
