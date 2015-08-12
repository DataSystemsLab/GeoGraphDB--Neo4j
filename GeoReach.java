package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.WebResource;

public class GeoReach implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	private static WebResource resource;
	
	GeoReach()
	{
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
	}
	
	// give a vertex id return a boolean value indicating whether it has RMBR
	public static boolean HasRMBR(int id)
	{
		return p_neo4j_graph_store.HasProperty(id, "RMBR_minx");
	}
	
	public static MyRectangle GetRMBR(int id)
	{
		MyRectangle RMBR = new MyRectangle();
		
		String query = "match (a) where id(a) = " + id + " return a.RMBR_minx, a.RMBR_miny, a.RMBR_maxx, a.RMBR_maxy";		
		ArrayList<String> result = Neo4j_Graph_Store.GetExecuteResultData(Neo4j_Graph_Store.Execute(resource, query));
		
		String data = result.get(0);
		String[] l = data.split(",");
		
		RMBR.min_x = Double.parseDouble(l[0]);
		RMBR.min_y = Double.parseDouble(l[1]);
		RMBR.max_x = Double.parseDouble(l[2]);
		RMBR.max_y = Double.parseDouble(l[3]);
		return RMBR;
	}
	
	//MBR operation of a given id vertex's current RMBR and another rectangle return a new rectangle, if no change happens it will return null
	public static MyRectangle MBR(int id,String minx2_s, String miny2_s, String maxx2_s, String maxy2_s)
	{	
		MyRectangle rec;				
			
		if(!HasRMBR(id))
		{	
			rec = new MyRectangle();
			rec.min_x = Double.parseDouble(minx2_s);
			rec.min_y = Double.parseDouble(miny2_s);
			rec.max_x = Double.parseDouble(maxx2_s);
			rec.max_y = Double.parseDouble(maxy2_s);
			return rec;
		}

		rec = GetRMBR(id);	
		
		double minx2 = Double.parseDouble(minx2_s);
		double miny2 = Double.parseDouble(miny2_s);
		double maxx2 = Double.parseDouble(maxx2_s);
		double maxy2 = Double.parseDouble(maxy2_s);
		
		boolean flag = false;
		if(minx2 < rec.min_x)
		{
			rec.min_x = minx2;
			flag = true;
		}
		
		if(miny2 < rec.min_y)
		{
			rec.min_y = miny2;
			flag = true;
		}
		
		if(maxx2 > rec.max_x)
		{
			rec.max_x = maxx2;
			flag = true;
		}
		
		if(maxy2 > rec.max_y)
		{
			rec.max_y = maxy2;
			flag = true;
		}
		
		if(flag)
			return rec;
		else
			return null;
	}	
	
	public void Preprocess()
	{	
		ArrayList<Integer> spatial_vertices = p_neo4j_graph_store.GetSpatialVertices();
		Queue<Integer> queue = new LinkedList<Integer>();
		HashSet<Integer> hs = new HashSet();
		for(int i = 0;i<spatial_vertices.size();i++)
		{
			int id = spatial_vertices.get(i);
			queue.add(id);
			hs.add(id);
		}
		
		while(!queue.isEmpty())
		{
			System.out.println(hs.size());
			//System.out.println(queue);
			int current_id = queue.poll();
			hs.remove(current_id);
			
			ArrayList<Integer> neighbors = p_neo4j_graph_store.GetInNeighbors(current_id);
			
			String latitude = null, longitude = null;
			
			String minx_s = null, miny_s = null, maxx_s = null, maxy_s = null;
			
			boolean isspatial = false;
			
			if(p_neo4j_graph_store.IsSpatial(current_id))
			{
				isspatial = true;
				
				double[] location = p_neo4j_graph_store.GetVerticeLocation(current_id);
				longitude = String.valueOf(location[0]);
				latitude = String.valueOf(location[1]);
			}
			
			boolean hasRMBR = false;
			
			if(HasRMBR(current_id))
			{
				hasRMBR = true;
				
				MyRectangle p_rec = GetRMBR(current_id);
				minx_s = String.valueOf(p_rec.min_x);
				miny_s = String.valueOf(p_rec.min_y);
				maxx_s = String.valueOf(p_rec.max_x);
				maxy_s = String.valueOf(p_rec.max_y);
			}
			
			
			for(int i = 0;i<neighbors.size();i++)
			{
				int neighbor = neighbors.get(i);
				boolean changed = false;
				
				if(isspatial)
				{
				
					MyRectangle new_RMBR = MBR(neighbor, longitude, latitude, longitude, latitude);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						
						String query = "match (a) where id(a) = " + neighbor + " set a.RMBR_minx = " + minx + ", a.RMBR_miny = " + miny + ", a.RMBR_maxx = " + maxx + ", a.RMBR_maxy = " + maxy;
						Neo4j_Graph_Store.Execute(resource, query);
					}
				}
				
				if(hasRMBR)
				{
					MyRectangle new_RMBR = MBR(neighbor, minx_s, miny_s, maxx_s, maxy_s);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						
						String query = "match (a) where id(a) = " + neighbor + " set a.RMBR_minx = " + minx + ", a.RMBR_miny = " + miny + ", a.RMBR_maxx = " + maxx + ", a.RMBR_maxy = " + maxy;
						Neo4j_Graph_Store.Execute(resource, query);
					}
				}
					
				if(changed&&!hs.contains(neighbor))
				{
					queue.add(neighbor);
					hs.add(neighbor);
				}
				
			}	
		}
	}
	
	static boolean TraversalQuery(int start_id, MyRectangle rect)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		String result = Neo4j_Graph_Store.Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");

		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}
			
			jsonObject = (JsonObject)row.get(1);
			if(jsonObject.has("longitude"))
			{
				double lat = Double.parseDouble(jsonObject.get("latitude").toString());
				double lon = Double.parseDouble(jsonObject.get("longitude").toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					System.out.println(id);
					return true;
				}
			}
			if(jsonObject.has("RMBR_minx"))
			{
				MyRectangle RMBR = new MyRectangle();
				RMBR.min_x = jsonObject.get("RMBR_minx").getAsDouble();
				RMBR.min_y = jsonObject.get("RMBR_miny").getAsDouble();
				RMBR.max_x = jsonObject.get("RMBR_maxx").getAsDouble();
				RMBR.max_y = jsonObject.get("RMBR_maxy").getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
					return true;
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
					false_count+=1;
			}
			else
				false_count+=1;

		}
	
		if(false_count == jsonArr.size())
			return false;
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
				continue;
			VisitedVertices.add(id);
			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery(int start_id, MyRectangle rect)
	{		
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		
		if(!all_attributes.has("RMBR_minx"))
			return false;
		
		String minx_s = String.valueOf(all_attributes.get("RMBR_minx"));
		String miny_s = String.valueOf(all_attributes.get("RMBR_miny"));
		String maxx_s = String.valueOf(all_attributes.get("RMBR_maxx"));
		String maxy_s = String.valueOf(all_attributes.get("RMBR_maxy"));
		
		MyRectangle RMBR = new MyRectangle();
										
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
			return false;
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
			return true;
		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		String result = Neo4j_Graph_Store.Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		int count = jsonArr.size();

		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}
		
			jsonObject = (JsonObject)row.get(1);
			if(jsonObject.has("longitude"))
			{
				double lat = Double.parseDouble(jsonObject.get("latitude").toString());
				double lon = Double.parseDouble(jsonObject.get("longitude").toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					System.out.println(id);
					return true;
				}
			}
			if(jsonObject.has("RMBR_minx"))
			{
				RMBR = new MyRectangle();
				RMBR.min_x = jsonObject.get("RMBR_minx").getAsDouble();
				RMBR.min_y = jsonObject.get("RMBR_miny").getAsDouble();
				RMBR.max_x = jsonObject.get("RMBR_maxx").getAsDouble();
				RMBR.max_y = jsonObject.get("RMBR_maxy").getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
					return true;
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
					false_count+=1;
			}
			else
				false_count+=1;

		}
		
		if(false_count == jsonArr.size())
			return false;
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
				continue;
			VisitedVertices.add(id);
			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;
			
		}
		
		return false;
	}
}