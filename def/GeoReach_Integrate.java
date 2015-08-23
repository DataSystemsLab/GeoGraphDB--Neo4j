package def;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.WebResource;

public class GeoReach_Integrate implements ReachabilityQuerySolver
{
	public MyRectangle total_range;
	public int split_pieces;
	public double resolution;
	
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	private static WebResource resource;
	
	public long neo4j_time;
	public long judge_time;
	
	public GeoReach_Integrate(MyRectangle rect, int p_split_pieces)
	{
		
		total_range = new MyRectangle();
		total_range.min_x = rect.min_x;
		total_range.min_y = rect.min_y;
		total_range.max_x = rect.max_x;
		total_range.max_y = rect.max_y;
		
		split_pieces = p_split_pieces;
		
		resolution = (total_range.max_x - total_range.min_x)/split_pieces;
		
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
		neo4j_time = 0;
		judge_time = 0;
	}
	
	public void Preprocess() {
		// TODO Auto-generated method stub
		
	}

	private boolean TraversalQuery(int start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
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
			if(jsonObject.has("longitude"))
			{
				double lat = Double.parseDouble(jsonObject.get("latitude").toString());
				double lon = Double.parseDouble(jsonObject.get("longitude").toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					judge_time += System.currentTimeMillis() - start;
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
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
				ArrayList<Integer> al = new Gson().fromJson(jsonObject.get("ReachGrid_5"), listType);
				HashSet<Integer> reachgrid = new HashSet<Integer>();
				for(int j = 0;j<al.size();j++)
				{
					reachgrid.add(al.get(j));
				}
				
				//ReachGrid totally Lie In query rectangle
				if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
				{
					for(int k = lb_x+1;k<rt_x;k++)
					{
						for(int j = lb_y+1;j<rt_y;j++)
						{
							int grid_id = k*split_pieces+j;
							if(reachgrid.contains(grid_id))
							{
								judge_time += System.currentTimeMillis() - start;
								return true;
							}
						}
					}
				}

				//ReachGrid No overlap with query rectangle
				boolean flag = false;
				for(int j = lb_y;j<=rt_y;j++)
				{
					int grid_id = lb_x*split_pieces+j;
					if(reachgrid.contains(grid_id))
						flag = true;
					grid_id = rt_x*split_pieces+j;
					if(reachgrid.contains(grid_id))
						flag = true;
				}
				for(int j = lb_x+1;j<rt_x;j++)
				{
					int grid_id = j*split_pieces+lb_y;
					if(reachgrid.contains(grid_id))
						flag = true;
					grid_id = j*split_pieces+rt_y;
					if(reachgrid.contains(grid_id))
						flag = true;
				}
				if(flag == false)
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
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			judge_time += System.currentTimeMillis() - start;
			boolean reachable = TraversalQuery(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has("RMBR_minx"))
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		String minx_s = String.valueOf(all_attributes.get("RMBR_minx"));
		String miny_s = String.valueOf(all_attributes.get("RMBR_miny"));
		String maxx_s = String.valueOf(all_attributes.get("RMBR_maxx"));
		String maxy_s = String.valueOf(all_attributes.get("RMBR_maxy"));
		
		MyRectangle RMBR = new MyRectangle();
										
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		//RMBR No overlap case
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		//RMBR Contain case
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			judge_time += System.currentTimeMillis() - start;
			return true;
		}
		
		//Grid Section
		Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
		ArrayList<Integer> al = new Gson().fromJson(all_attributes.get("ReachGrid_5"), listType);
		HashSet<Integer> reachgrid = new HashSet<Integer>();
		for(int i = 0;i<al.size();i++)
		{
			reachgrid.add(al.get(i));
		}
		
		int lb_x = (int) ((rect.min_x - total_range.min_x)/resolution);
		int lb_y = (int) ((rect.min_y - total_range.min_y)/resolution);
		int rt_x = (int) ((rect.max_x - total_range.min_x)/resolution);
		int rt_y = (int) ((rect.max_y - total_range.min_y)/resolution);
		
		//ReachGrid totally Lie In query rectangle
		if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
		{
			for(int i = lb_x+1;i<rt_x;i++)
			{
				for(int j = lb_y+1;j<rt_y;j++)
				{
					int grid_id = i*split_pieces+j;
					if(reachgrid.contains(grid_id))
					{
						judge_time += System.currentTimeMillis() - start;
						return true;
					}
				}
			}
		}

		//ReachGrid No overlap with query rectangle
		boolean flag = false;
		for(int i = lb_y;i<=rt_y;i++)
		{
			int grid_id = lb_x*split_pieces+i;
			if(reachgrid.contains(grid_id))
				flag = true;
			grid_id = rt_x*split_pieces+i;
			if(reachgrid.contains(grid_id))
				flag = true;
		}
		for(int i = lb_x+1;i<rt_x;i++)
		{
			int grid_id = i*split_pieces+lb_y;
			if(reachgrid.contains(grid_id))
				flag = true;
			grid_id = i*split_pieces+rt_y;
			if(reachgrid.contains(grid_id))
				flag = true;
		}
		if(flag == false)
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
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
			if(jsonObject.has("longitude"))
			{
				double lat = Double.parseDouble(jsonObject.get("latitude").toString());
				double lon = Double.parseDouble(jsonObject.get("longitude").toString());
				if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
				{
					judge_time += System.currentTimeMillis() - start;
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
				
				if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
				{
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				al = new Gson().fromJson(jsonObject.get("ReachGrid_5"), listType);
				reachgrid = new HashSet<Integer>();
				for(int j = 0;j<al.size();j++)
				{
					reachgrid.add(al.get(j));
				}
				
				//ReachGrid totally Lie In query rectangle
				if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
				{
					for(int k = lb_x+1;k<rt_x;k++)
					{
						for(int j = lb_y+1;j<rt_y;j++)
						{
							int grid_id = k*split_pieces+j;
							if(reachgrid.contains(grid_id))
							{
								judge_time += System.currentTimeMillis() - start;
								return true;
							}
						}
					}
				}

				//ReachGrid No overlap with query rectangle
				flag = false;
				for(int j = lb_y;j<=rt_y;j++)
				{
					int grid_id = lb_x*split_pieces+j;
					if(reachgrid.contains(grid_id))
						flag = true;
					grid_id = rt_x*split_pieces+j;
					if(reachgrid.contains(grid_id))
						flag = true;
				}
				for(int j = lb_x+1;j<rt_x;j++)
				{
					int grid_id = j*split_pieces+lb_y;
					if(reachgrid.contains(grid_id))
						flag = true;
					grid_id = j*split_pieces+rt_y;
					if(reachgrid.contains(grid_id))
						flag = true;
				}
				if(flag == false)
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
			jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			if(VisitedVertices.contains(id))
			{
				judge_time += System.currentTimeMillis() - start;
				continue;
			}
			VisitedVertices.add(id);
			judge_time += System.currentTimeMillis() - start;
			boolean reachable = TraversalQuery(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		
		return false;
	}
	
	
}
