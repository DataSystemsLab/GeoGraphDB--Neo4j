package def;

import java.util.HashSet;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.jersey.api.client.WebResource;

public class ReachabilityIndex {
	
	private String longitude_property_name = "longitude";
	private String latitude_property_name = "latitude";
	
	public long GetTranTime;	
	public long JudgeTime;
	
	private Neo4j_Graph_Store p_neo;
	private WebResource resource;
	
	public ReachabilityIndex(String p_suffix)
	{
		Config config = new Config(p_suffix);
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		p_neo = new Neo4j_Graph_Store(p_suffix);
		resource = p_neo.GetCypherResource();
		
		GetTranTime = 0;		
		JudgeTime = 0;
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect, String TransitiveClosureLabel, String GraphLabel)
	{
		if(TransitiveClosureLabel == null)
			TransitiveClosureLabel = "Transitive_Closure";
		long start = System.currentTimeMillis();
		String attribute_id = p_neo.GetVertexAttributeValue(start_id, "id");
		String query = "match (a:" + TransitiveClosureLabel + ") -->(b) where a.id = " + attribute_id + " return b.id";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		HashSet<Integer> reach_nodes = Neo4j_Graph_Store.GetExecuteResultDataInSet(result);
		
		GetTranTime += System.currentTimeMillis() - start;
		if(reach_nodes.size() == 0)
		{
			JudgeTime+=System.currentTimeMillis() - start;
			return false;
		}
		start = System.currentTimeMillis();
		int bulksize = 2000;
		int bulkcount = reach_nodes.size() / bulksize;
		Iterator<Integer> iter = reach_nodes.iterator();
		for(int i = 0;i<bulkcount;i++)
		{
			query = "match (a:" + GraphLabel + ") where a.id in [" + iter.next();
			for(int j = 1;j<bulksize;j++)
			{
				query += ("," + iter.next());
			}
			query+=("] return a");
			result = Neo4j_Graph_Store.Execute(resource, query);
			JsonArray jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			for(int j = 0;j<jsonArr.size();j++)
			{
				JsonObject jsonOb = (JsonObject) jsonArr.get(j);
				JsonArray arr = jsonOb.get("row").getAsJsonArray();
				jsonOb = arr.get(0).getAsJsonObject();
				if(jsonOb.has(longitude_property_name))
				{
					double lon = jsonOb.get(longitude_property_name).getAsDouble();
					double lat = jsonOb.get(latitude_property_name).getAsDouble();
					if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
					{
						JudgeTime += System.currentTimeMillis() - start;
						return true;
					}
				}
			}
		}
		if(iter.hasNext())
		{
			query = "match (a:" + GraphLabel + ") where a.id in [" + iter.next();
			for(int i = bulkcount * bulksize + 1;i< reach_nodes.size();i++)
				query += ("," + iter.next());
			query += "] return a";
			result = Neo4j_Graph_Store.Execute(resource, query);
			JsonArray jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			for(int j = 0;j<jsonArr.size();j++)
			{
				JsonObject jsonOb = (JsonObject) jsonArr.get(j);
				JsonArray arr = jsonOb.get("row").getAsJsonArray();
				jsonOb = arr.get(0).getAsJsonObject();
				if(jsonOb.has(longitude_property_name))
				{
					double lon = jsonOb.get(longitude_property_name).getAsDouble();
					double lat = jsonOb.get(latitude_property_name).getAsDouble();
					if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
					{
						JudgeTime += System.currentTimeMillis() - start;
						return true;
					}
				}
			}
		}
			
		JudgeTime += System.currentTimeMillis() - start;
		return false;
	}

}

