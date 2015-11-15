package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.WebResource;

public class GeoReach implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	public Neo4j_Graph_Store p_neo4j_graph_store;
	private static WebResource resource;
	
	private String longitude_property_name;
	private String latitude_property_name;
	
	private String RMBR_minx_name;
	private String RMBR_miny_name;
	private String RMBR_maxx_name;
	private String RMBR_maxy_name;
	
	public long neo4j_time;
	public long judge_time;
	
	
	public GeoReach(String p_suffix, int ratio)
	{
		p_neo4j_graph_store = new Neo4j_Graph_Store(p_suffix);
		resource = p_neo4j_graph_store.GetCypherResource();
		
		Config config = new Config(p_suffix);
		longitude_property_name = config.GetLongitudePropertyName()+"_"+ratio;
		latitude_property_name = config.GetLatitudePropertyName()+"_"+ratio;
		RMBR_minx_name = config.GetRMBR_minx_name()+"_"+ratio;
		RMBR_miny_name = config.GetRMBR_miny_name()+"_"+ratio;
		RMBR_maxx_name = config.GetRMBR_maxx_name()+"_"+ratio;
		RMBR_maxy_name = config.GetRMBR_maxy_name()+"_"+ratio;
		
		neo4j_time = 0;
		judge_time = 0;
		
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
		String query = String.format("match (a)-->(b) where id(a) = %d return b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, id(b)", start_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name, longitude_property_name, latitude_property_name);
		
		long start = System.currentTimeMillis();
		String result = p_neo4j_graph_store.Execute(query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(6).getAsInt();
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}
			
			if(!row.get(4).isJsonNull())
			{
				double lon = row.get(4).getAsDouble();
				double lat = row.get(5).getAsDouble();
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(!row.get(0).isJsonNull())
			{
				MyRectangle RMBR = new MyRectangle();
				RMBR.min_x = row.get(0).getAsDouble();
				RMBR.min_y = row.get(1).getAsDouble();
				RMBR.max_x = row.get(2).getAsDouble();
				RMBR.max_y = row.get(3).getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					judge_time += System.currentTimeMillis() - start;
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
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		judge_time += System.currentTimeMillis() - start;
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			start = System.currentTimeMillis();
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(6).getAsInt();
			if(VisitedVertices.contains(id))
			{
				judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			judge_time += System.currentTimeMillis() - start;
			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery(long start_id, MyRectangle rect)
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		String query = String.format("match (n) where id(n) = %d return n.%s, n.%s, n.%s, n.%s", start_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		jsonArr = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
		neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		if(jsonArr.get(0).isJsonNull())
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		String minx_s = (jsonArr.get(0).getAsString());
		String miny_s = (jsonArr.get(1).getAsString());
		String maxx_s = (jsonArr.get(2).getAsString());
		String maxy_s = (jsonArr.get(3).getAsString());
				
		MyRectangle RMBR = new MyRectangle();
										
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			judge_time += System.currentTimeMillis() - start;
			return true;
		}
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		query = String.format("match (a)-->(b) where id(a) = %d return b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, id(b)", start_id, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name, RMBR_maxy_name, longitude_property_name, latitude_property_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		neo4j_time += System.currentTimeMillis() - start;		
		
		start = System.currentTimeMillis();
		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(6).getAsInt();
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}
		
			if(!row.get(4).isJsonNull())
			{
				double lon = row.get(4).getAsDouble();
				double lat = row.get(5).getAsDouble();

				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(!row.get(0).isJsonNull())
			{
				RMBR = new MyRectangle();
				RMBR.min_x = row.get(0).getAsDouble();
				RMBR.min_y = row.get(1).getAsDouble();
				RMBR.max_x = row.get(2).getAsDouble();
				RMBR.max_y = row.get(3).getAsDouble();
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					judge_time += System.currentTimeMillis() - start;
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
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		judge_time += System.currentTimeMillis() - start;
		start = System.currentTimeMillis();
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(6).getAsInt();
			if(VisitedVertices.contains(id))
			{
				judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			judge_time += System.currentTimeMillis() - start;
 			boolean reachable = TraversalQuery(id, rect);
			
			if(reachable)
				return true;
			
		}
		
		return false;
	}
}