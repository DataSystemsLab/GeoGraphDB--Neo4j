package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.jersey.api.client.WebResource;

public class GeoReach implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	private static WebResource resource;
	
	private String longitude_property_name;
	private String latitude_property_name;
	
	private String RMBR_minx_name;
	private String RMBR_miny_name;
	private String RMBR_maxx_name;
	private String RMBR_maxy_name;
	
	public long query_neo4j_time;
	public long query_judge_time;
	
	public long update_addedge_time;
	public long update_deleteedge_time;
	
	public long update_neo4j_time;
	public long update_inmemory_time;
	
	public GeoReach()
	{
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
		
		Config config = new Config();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		RMBR_minx_name = config.GetRMBR_minx_name();
		RMBR_miny_name = config.GetRMBR_miny_name();
		RMBR_maxx_name = config.GetRMBR_maxx_name();
		RMBR_maxy_name = config.GetRMBR_maxy_name();
		
		query_neo4j_time = 0;
		query_judge_time = 0;
		update_addedge_time = 0;
		update_deleteedge_time = 0;
		update_neo4j_time = 0;
		update_inmemory_time = 0;
	}
	
	// give a vertex id return a boolean value indicating whether it has RMBR
	public boolean HasRMBR(int id)
	{
		return p_neo4j_graph_store.HasProperty(id, RMBR_minx_name);
	}
	
	public MyRectangle GetRMBR(int id)
	{
		MyRectangle RMBR = new MyRectangle();
		
		String query = "match (a) where id(a) = " + id + " return a."+RMBR_minx_name+", a."+RMBR_miny_name+", a."+RMBR_maxx_name+", a."+RMBR_maxy_name;		
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
	public MyRectangle MBR(int id,String minx2_s, String miny2_s, String maxx2_s, String maxy2_s)
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
	
	public MyRectangle MBR(MyRectangle start_rect, MyRectangle end_rect)
	{
		if(start_rect == null)
			return end_rect;
		else
		{
			if(end_rect == null)
				return start_rect;
			else
			{
				if(end_rect.min_x<start_rect.min_x)
					start_rect.min_x = end_rect.min_x;
				if(end_rect.min_y<start_rect.min_y)
					start_rect.min_y = end_rect.min_y;
				if(end_rect.max_x>start_rect.max_x)
					start_rect.max_x = end_rect.max_x;
				if(end_rect.max_y>start_rect.max_y)
					start_rect.max_y = end_rect.max_y;
				return start_rect;
			}
		}
		
	}
	
	public void Preprocess()
	{	
		ArrayList<Integer> spatial_vertices = p_neo4j_graph_store.GetSpatialVertices();
		Queue<Integer> queue = new LinkedList<Integer>();
		HashSet<Integer> hs = new HashSet<Integer>();
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
						
						String query = "match (a) where id(a) = " + neighbor + " set a."+RMBR_minx_name+" = " + minx + ", a."+RMBR_miny_name+" = " + miny + ", a."+RMBR_maxx_name+" = " + maxx + ", a."+RMBR_maxy_name+" = " + maxy;
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
						
						String query = "match (a) where id(a) = " + neighbor + " set a."+RMBR_minx_name+" = " + minx + ", a."+RMBR_miny_name+" = " + miny + ", a."+RMBR_maxx_name+" = " + maxx + ", a."+RMBR_maxy_name+" = " + maxy;
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
	
	private boolean TraversalQuery(int start_id, MyRectangle rect)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");

		start = System.currentTimeMillis();
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
			if(jsonObject.has(longitude_property_name))
			{
				double lat = Double.parseDouble(jsonObject.get(latitude_property_name).toString());
				double lon = Double.parseDouble(jsonObject.get(longitude_property_name).toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					query_judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(jsonObject.has(RMBR_minx_name))
			{
				MyRectangle RMBR = new MyRectangle();
				RMBR.min_x = jsonObject.get(RMBR_minx_name).getAsDouble();
				RMBR.min_y = jsonObject.get(RMBR_miny_name).getAsDouble();
				RMBR.max_x = jsonObject.get(RMBR_maxx_name).getAsDouble();
				RMBR.max_y = jsonObject.get(RMBR_maxy_name).getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					query_judge_time += System.currentTimeMillis() - start;
					return true;
				}
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
				}
			}
			else
			{
				false_count+=1;
				VisitedVertices.add(id);
			}
		}
	
		if(false_count == jsonArr.size())
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		query_judge_time += System.currentTimeMillis() - start;
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			start = System.currentTimeMillis();
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				query_judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			query_judge_time += System.currentTimeMillis() - start;
			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery(int start_id, MyRectangle rect)
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(RMBR_minx_name))
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		String minx_s = String.valueOf(all_attributes.get(RMBR_minx_name));
		String miny_s = String.valueOf(all_attributes.get(RMBR_miny_name));
		String maxx_s = String.valueOf(all_attributes.get(RMBR_maxx_name));
		String maxy_s = String.valueOf(all_attributes.get(RMBR_maxy_name));
		
		MyRectangle RMBR = new MyRectangle();
										
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
			return true;
		}
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		start = System.currentTimeMillis();
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
			if(jsonObject.has(longitude_property_name))
			{
				double lat = Double.parseDouble(jsonObject.get(latitude_property_name).toString());
				double lon = Double.parseDouble(jsonObject.get(longitude_property_name).toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					query_judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(jsonObject.has(RMBR_minx_name))
			{
				RMBR = new MyRectangle();
				RMBR.min_x = jsonObject.get(RMBR_minx_name).getAsDouble();
				RMBR.min_y = jsonObject.get(RMBR_miny_name).getAsDouble();
				RMBR.max_x = jsonObject.get(RMBR_maxx_name).getAsDouble();
				RMBR.max_y = jsonObject.get(RMBR_maxy_name).getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					query_judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
				}
			}
			else
			{
				false_count+=1;
				VisitedVertices.add(id);
			}

		}
		
		if(false_count == jsonArr.size())
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		query_judge_time += System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				query_judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			query_judge_time += System.currentTimeMillis() - start;
			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;
			
		}
		
		return false;
	}
	
	//used in UpdateAddEdge to recursively update changing of RMBR(the end_id must be changed if the function is called and jArr_end store end_id's location and RMBR in a jsonArray)
	public void UpdateAddEdgeTraverse(long end_id, JsonArray jArr_end)
	{
		String query = null;
		String result = null;
				
		long start = System.currentTimeMillis();
		query = String.format("match (n)-->(b) where id(b) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s, id(n)", end_id, longitude_property_name, latitude_property_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time+=System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		for(int j_index = 0;j_index<jsonArr.size();j_index++)
		{
			JsonArray jArr_start = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
			long start_id = jArr_start.get(6).getAsLong();
			boolean flag = false;
			
			MyRectangle update_RMBR = null;
			//start node has no RMBR
			if(jArr_start.get(2).isJsonNull())
			{
				//end node has location
				if(!jArr_end.get(0).isJsonNull())
				{
					flag = true;
					double lon = jArr_end.get(0).getAsDouble();
					double lat = jArr_end.get(1).getAsDouble();
					
					update_RMBR = new MyRectangle(lon, lat, lon, lat);
					
					//end node has RMBR
					if(!jArr_end.get(2).isJsonNull())
					{
						MyRectangle end_RMBR = new MyRectangle(jArr_end.get(2).getAsDouble(),jArr_end.get(3).getAsDouble(),jArr_end.get(4).getAsDouble(),jArr_end.get(5).getAsDouble());
						if(lon<end_RMBR.max_x)
							update_RMBR.max_x = end_RMBR.max_x;
						if(lon>end_RMBR.min_x)
							update_RMBR.min_x = end_RMBR.min_x;
						if(lat<end_RMBR.max_y)
							update_RMBR.max_y = end_RMBR.max_y;
						if(lat>end_RMBR.min_y)
							update_RMBR.min_y = end_RMBR.min_y;
					}
					
				}
				//end node has no location
				else
				{
					//end node has RMBR
					if(!jArr_end.get(2).isJsonNull())
					{
						flag = true;
						update_RMBR = new MyRectangle(jArr_end.get(2).getAsDouble(),jArr_end.get(3).getAsDouble(),jArr_end.get(4).getAsDouble(),jArr_end.get(5).getAsDouble());
					}
				}
			}
			//start node has RMBR
			else
			{
				update_RMBR = new MyRectangle(jArr_start.get(2).getAsDouble(),jArr_start.get(3).getAsDouble(),jArr_start.get(4).getAsDouble(),jArr_start.get(5).getAsDouble());
				if(!jArr_end.get(0).isJsonNull())
				{
					double lon = jArr_end.get(0).getAsDouble();
					double lat = jArr_end.get(1).getAsDouble();
					
					if(lon>update_RMBR.max_x)
					{
						flag = true;
						update_RMBR.max_x = lon;
					}
					if(lon<update_RMBR.min_x)
					{
						flag = true;
						update_RMBR.min_x = lon;
					}
					if(lat>update_RMBR.max_y)
					{
						flag = true;
						update_RMBR.max_y = lat;
					}
					if(lat<update_RMBR.min_y)
					{
						flag = true;
						update_RMBR.min_y = lat;
					}
				}
				if(!jArr_end.get(2).isJsonNull())
				{
					double minx = jArr_end.get(2).getAsDouble();
					double miny = jArr_end.get(3).getAsDouble();
					double maxx = jArr_end.get(4).getAsDouble();
					double maxy = jArr_end.get(5).getAsDouble();
					if(minx<update_RMBR.min_x)
					{
						flag = true;
						update_RMBR.min_x = minx;
					}
					if(miny<update_RMBR.min_y)
					{
						flag = true;
						update_RMBR.min_y = miny;
					}
					if(maxx>update_RMBR.max_x)
					{
						flag = true;
						update_RMBR.max_x = maxx;
					}
					if(maxy>update_RMBR.max_y)
					{
						flag = true;
						update_RMBR.max_y = maxy;
					}
				}
			}
			update_inmemory_time+=System.currentTimeMillis() - start;
			
			if(flag)
			{
				start = System.currentTimeMillis();
				query = String.format("match (n) where id(n) = %d set n.%s = %.8f, n.%s=%.8f, n.%s=%.8f, n.%s=%.8f", start_id, RMBR_minx_name, update_RMBR.min_x, RMBR_miny_name, update_RMBR.min_y, RMBR_maxx_name, update_RMBR.max_x, RMBR_maxy_name, update_RMBR.max_y);
				Neo4j_Graph_Store.Execute(resource, query);
				update_neo4j_time += System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				jArr_start.set(2, new JsonPrimitive(update_RMBR.min_x));
				jArr_start.set(3, new JsonPrimitive(update_RMBR.min_y));
				jArr_start.set(4, new JsonPrimitive(update_RMBR.max_x));
				jArr_start.set(5, new JsonPrimitive(update_RMBR.max_y));
				update_inmemory_time += System.currentTimeMillis() - start;
				UpdateAddEdgeTraverse(start_id, jArr_start);			
			}
		}
	}
	
	public boolean UpdateAddEdge(long start_id, long end_id)
	{
		long start = System.currentTimeMillis();
		String query = String.format("match (a),(b) where id(a) = %d and id(b) = %d create (a)-[:Added_Edge]->(b)", start_id, end_id);
		Neo4j_Graph_Store.Execute(resource, query);
		update_addedge_time+=System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		query = String.format("match (n) where id(n) in [%d,%d] return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", start_id, end_id, longitude_property_name, latitude_property_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time+=System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		JsonArray jArr_start = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
		JsonArray jArr_end = jsonArr.get(1).getAsJsonObject().get("row").getAsJsonArray();
		
		boolean flag = false;
		
		MyRectangle update_RMBR = null;
		//start node has no RMBR
		if(jArr_start.get(2).isJsonNull())
		{
			//end node has location
			if(!jArr_end.get(0).isJsonNull())
			{
				flag = true;
				double lon = jArr_end.get(0).getAsDouble();
				double lat = jArr_end.get(1).getAsDouble();
				
				update_RMBR = new MyRectangle(lon, lat, lon, lat);
				
				//end node has RMBR
				if(!jArr_end.get(2).isJsonNull())
				{
					MyRectangle end_RMBR = new MyRectangle(jArr_end.get(2).getAsDouble(),jArr_end.get(3).getAsDouble(),jArr_end.get(4).getAsDouble(),jArr_end.get(5).getAsDouble());
					if(lon<end_RMBR.max_x)
						update_RMBR.max_x = end_RMBR.max_x;
					if(lon>end_RMBR.min_x)
						update_RMBR.min_x = end_RMBR.min_x;
					if(lat<end_RMBR.max_y)
						update_RMBR.max_y = end_RMBR.max_y;
					if(lat>end_RMBR.min_y)
						update_RMBR.min_y = end_RMBR.min_y;
				}
			}
			//end node has no location
			else
			{
				//end node has RMBR
				if(!jArr_end.get(2).isJsonNull())
				{
					flag = true;
					update_RMBR = new MyRectangle(jArr_end.get(2).getAsDouble(),jArr_end.get(3).getAsDouble(),jArr_end.get(4).getAsDouble(),jArr_end.get(5).getAsDouble());
				}
			}
		}
		//start node has RMBR
		else
		{
			update_RMBR = new MyRectangle(jArr_start.get(2).getAsDouble(),jArr_start.get(3).getAsDouble(),jArr_start.get(4).getAsDouble(),jArr_start.get(5).getAsDouble());
			if(!jArr_end.get(0).isJsonNull())
			{
				double lon = jArr_end.get(0).getAsDouble();
				double lat = jArr_end.get(1).getAsDouble();
				
				if(lon>update_RMBR.max_x)
				{
					flag = true;
					update_RMBR.max_x = lon;
				}
				if(lon<update_RMBR.min_x)
				{
					flag = true;
					update_RMBR.min_x = lon;
				}if(lat>update_RMBR.max_y)
				{
					flag = true;
					update_RMBR.max_y = lat;
				}if(lat<update_RMBR.min_y)
				{
					flag = true;
					update_RMBR.min_y = lat;
				}
			}
			if(!jArr_end.get(2).isJsonNull())
			{
				double minx = jArr_end.get(2).getAsDouble();
				double miny = jArr_end.get(3).getAsDouble();
				double maxx = jArr_end.get(4).getAsDouble();
				double maxy = jArr_end.get(5).getAsDouble();
				if(minx<update_RMBR.min_x)
				{
					flag = true;
					update_RMBR.min_x = minx;
				}
				if(miny<update_RMBR.min_y)
				{
					flag = true;
					update_RMBR.min_y = miny;
				}
				if(maxx>update_RMBR.max_x)
				{
					flag = true;
					update_RMBR.max_x = maxx;
				}
				if(maxy>update_RMBR.max_y)
				{
					flag = true;
					update_RMBR.max_y = maxy;
				}
			}
		}
		
		update_inmemory_time+=System.currentTimeMillis() - start;
		if(flag)
		{
			start = System.currentTimeMillis();
			query = String.format("match (n) where id(n) = %d set n.%s = %.8f, n.%s=%.8f, n.%s=%.8f, n.%s=%.8f", start_id, RMBR_minx_name, update_RMBR.min_x, RMBR_miny_name, update_RMBR.min_y, RMBR_maxx_name, update_RMBR.max_x, RMBR_maxy_name, update_RMBR.max_y);
			Neo4j_Graph_Store.Execute(resource, query);
			update_neo4j_time+=System.currentTimeMillis()-start;
			
			start = System.currentTimeMillis();
			jArr_start.set(2, new JsonPrimitive(update_RMBR.min_x));
			jArr_start.set(3, new JsonPrimitive(update_RMBR.min_y));
			jArr_start.set(4, new JsonPrimitive(update_RMBR.max_x));
			jArr_start.set(5, new JsonPrimitive(update_RMBR.max_y));
			update_inmemory_time+=System.currentTimeMillis() - start;
			UpdateAddEdgeTraverse(start_id, jArr_start);			
		}
		return flag;
	}
	
	public MyRectangle Reconstruct(long start_id)
	{
		String query = null;
		String result = null;
		MyRectangle update_rect = null;
		
		long start = System.currentTimeMillis();
		query = String.format("match (a)-->(n) where id(a) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", start_id, longitude_property_name, latitude_property_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		for(int j_index = 0;j_index<jsonArr.size();j_index++)
		{
			JsonArray j_end = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
			if(!j_end.get(0).isJsonNull())
			{
				MyRectangle rect = new MyRectangle(j_end.get(0).getAsDouble(), j_end.get(1).getAsDouble(), j_end.get(0).getAsDouble(), j_end.get(1).getAsDouble());
				update_rect = MBR(update_rect, rect);
			}
			if(!j_end.get(2).isJsonNull())
			{
				MyRectangle rect = new MyRectangle(j_end.get(2).getAsDouble(), j_end.get(3).getAsDouble(), j_end.get(4).getAsDouble(), j_end.get(5).getAsDouble());
				update_rect = MBR(update_rect, rect);
			}
		}
		update_inmemory_time += System.currentTimeMillis() - start;
		return update_rect;
	}
	
	//use reconstruct to check whether the union requirement satisfies
	public boolean CheckRMBR(long id)
	{
		MyRectangle update = Reconstruct(id);
		String query = String.format("match (a) where id(a) = %d return a", id);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jarr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		JsonObject jobj  = jarr.get(0).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsJsonObject();
		if(!jobj.has(RMBR_minx_name))
		{
			if(update == null)
				return true;
			else
				return false;
		}
		else
		{
			if(update == null)
				return false;
			else
			{
				if(Math.abs(jobj.get(RMBR_minx_name).getAsDouble()-update.min_x)>0.00000001||Math.abs(jobj.get(RMBR_miny_name).getAsDouble()-update.min_y)>0.00000001||Math.abs(jobj.get(RMBR_maxx_name).getAsDouble()-update.max_x)>0.00000001||Math.abs(jobj.get(RMBR_maxy_name).getAsDouble()-update.max_y)>0.00000001)
				{
					return false;
				}
				else
					return true;
				}
		}
	}
	
	public void UpdateDeleteEdge_Traverse(long end_id)
	{
		String query = null;
		String result = null;
		
		long start = System.currentTimeMillis();
		query = String.format("match (a)-->(b) where id(b) = %d return a.%s, a.%s, a.%s, a.%s, id(a)", end_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		for(int j_index = 0;j_index<jsonArr.size();j_index++)
		{
			JsonArray j_start = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
			MyRectangle rect = new MyRectangle(j_start.get(0).getAsDouble(), j_start.get(1).getAsDouble(), j_start.get(2).getAsDouble(), j_start.get(3).getAsDouble());
			long start_id = j_start.get(4).getAsLong();
			MyRectangle update_RMBR = Reconstruct(start_id);
			if(update_RMBR == null)
			{
				if(j_start.get(0).isJsonNull())
					continue;
				else
				{
					update_inmemory_time += System.currentTimeMillis() - start;
					
					start = System.currentTimeMillis();
					query = String.format("match (n) where id(n) = %d, remove n.%s, n.%s, n.%s, n.%s", start_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
					Neo4j_Graph_Store.Execute(resource, query);
					update_neo4j_time += System.currentTimeMillis() - start;
					
					UpdateDeleteEdge_Traverse(start_id);
					start = System.currentTimeMillis();
				}
			}
			else
			{
				double ori_minx = j_start.get(0).getAsDouble();
				double ori_miny = j_start.get(1).getAsDouble();
				double ori_maxx = j_start.get(2).getAsDouble();
				double ori_maxy = j_start.get(3).getAsDouble();
				if(Math.abs(update_RMBR.min_x-ori_minx)>0.00000001||Math.abs(update_RMBR.min_y-ori_miny)>0.00000001||Math.abs(update_RMBR.min_x-ori_maxx)>0.00000001||Math.abs(update_RMBR.min_x-ori_maxy)>0.00000001)
				{
					update_inmemory_time += System.currentTimeMillis() - start;

					start = System.currentTimeMillis();
					query = String.format("match (n) where id(n)=%d set n.%s = %.8f, n.%s=%.8f, n.%s=%.8f, n.%s=%.8f", start_id, RMBR_minx_name, update_RMBR.min_x, RMBR_miny_name, update_RMBR.min_y, RMBR_maxx_name, update_RMBR.max_x, RMBR_maxy_name, update_RMBR.max_y);
					Neo4j_Graph_Store.Execute(resource, query);
					update_neo4j_time += System.currentTimeMillis() - start;

					UpdateDeleteEdge_Traverse(start_id);
					start = System.currentTimeMillis();
				}
			}
		}
		update_inmemory_time += System.currentTimeMillis() - start;
	}
	
	public void UpdateDeleteEdge(long start_id, long end_id)
	{
		String query = null;
		String result = null;
		
		long start = System.currentTimeMillis();
		query = String.format("match p = (a)-[r]->(b) where id(a) = %d and id(b) = %d delete r", start_id, end_id);
		Neo4j_Graph_Store.Execute(resource, query);
		update_deleteedge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		query = String.format("match (n) where id(n) in [%d,%d] return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", start_id, end_id, longitude_property_name, latitude_property_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		JsonArray jArr_start = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
		JsonArray jArr_end = jsonArr.get(1).getAsJsonObject().get("row").getAsJsonArray();
			
		if(!jArr_start.get(2).isJsonNull())
		{
			boolean flag = false;//a flag to represent whether the start vertex will be influenced by the deletion
			double minx = jArr_start.get(2).getAsDouble();
			double miny = jArr_start.get(3).getAsDouble();
			double maxx = jArr_start.get(4).getAsDouble();
			double maxy = jArr_start.get(5).getAsDouble();
			
			if(!jArr_end.get(0).isJsonNull())
			{
				double lon = jArr_end.get(0).getAsDouble();
				double lat = jArr_end.get(1).getAsDouble();
				
				if(Math.abs(lon-minx)<0.00000001||Math.abs(lon-maxx)<0.00000001||Math.abs(lat-miny)<0.00000001||Math.abs(lat-maxy)<0.00000001)
					flag = true;
			}
			if(!jArr_end.get(2).isJsonNull())
			{
				double minx_end = jArr_end.get(2).getAsDouble();
				double miny_end = jArr_end.get(3).getAsDouble();
				double maxx_end = jArr_end.get(4).getAsDouble();
				double maxy_end = jArr_end.get(5).getAsDouble();
				if(Math.abs(minx_end-minx)<0.00000001||Math.abs(maxx_end-maxx)<0.00000001||Math.abs(miny_end-miny)<0.00000001||Math.abs(maxy_end-maxy)<0.00000001)
					flag = true;
			}
			update_inmemory_time += System.currentTimeMillis() - start;
			
			if(flag)
			{
				MyRectangle update_RMBR = Reconstruct(start_id);
				if(update_RMBR == null)
				{
					query = String.format("match (n) where id(n)=%d remove n.%s, n.%s, n.%s, n.%s", start_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
					UpdateDeleteEdge_Traverse(start_id);
				}
				else
				{
					if(Math.abs(update_RMBR.min_x-minx)>0.00000001||Math.abs(update_RMBR.max_x-maxx)>0.00000001||Math.abs(update_RMBR.min_y-miny)>0.00000001||Math.abs(update_RMBR.max_y-maxy)>0.00000001)
					{
						query = String.format("match (n) where id(n)=%d set n.%s = %.8f, n.%s=%.8f, n.%s=%.8f, n.%s=%.8f", start_id, RMBR_minx_name, update_RMBR.min_x, RMBR_miny_name, update_RMBR.min_y, RMBR_maxx_name, update_RMBR.max_x, RMBR_maxy_name, update_RMBR.max_y);
						UpdateDeleteEdge_Traverse(start_id);
					}
				}
			}
		}
		
		
		
	}
}