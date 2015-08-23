package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.WebResource;

public class Geo_Reach_Grid implements ReachabilityQuerySolver{

public static Set<Integer> VisitedVertices = new HashSet();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	private static WebResource resource;
	
	public long neo4j_time;
	public long judge_time;
	
	public MyRectangle total_range;
	public int split_pieces;
	public double resolution;
	
	public Geo_Reach_Grid(MyRectangle rect, int p_split_pieces)
	{
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
		
		total_range = new MyRectangle();
		total_range.min_x = rect.min_x;
		total_range.min_y = rect.min_y;
		total_range.max_x = rect.max_x;
		total_range.max_y = rect.max_y;
		
		split_pieces = p_split_pieces;		
		resolution = (total_range.max_x - total_range.min_x)/split_pieces;
		
		neo4j_time = 0;
		judge_time = 0;
	}
	
	
	public static void LoadIndex(int split_pieces, String datasource)
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
		int node_count = OwnMethods.GetNodeCount(datasource);
		
		for(int ratio = 40;ratio<100;ratio+=20)
//		int ratio = 20;
		{
			long offset = ratio / 20 * node_count;
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+".txt");
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					int count = Integer.parseInt(l[1]);
					if(count == 0)
						continue;
					else
					{
						Map<String, Object> properties = new HashMap<String, Object>();
						int[] l_i = new int[l.length-2];
						for(int i = 2;i<l.length;i++)
							l_i[i-2] = Integer.parseInt(l[i]);
						properties.put("ReachGrid_"+split_pieces, l_i);
						inserter.setNodeProperty(id + offset, "ReachGrid_"+split_pieces, l_i);
					}

				}
				reader.close();	
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(inserter!=null)
					inserter.shutdown();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e)
					{					
					}
				}
			}
		}	
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
			if(jsonObject.has("ReachGrid_5"))
			{
				Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
				ArrayList<Integer> al = new Gson().fromJson(jsonObject.get("ReachGrid_"+split_pieces), listType);
				HashSet<Integer> reachgrid = new HashSet<Integer>();
				for(int j = 0;j<al.size();j++)
				{
					reachgrid.add(al.get(j));
				}
				
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

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) {
		// TODO Auto-generated method stub		
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has("ReachGrid_"+split_pieces))
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
		ArrayList<Integer> al = new Gson().fromJson(all_attributes.get("ReachGrid_"+split_pieces), listType);
		HashSet<Integer> reachgrid = new HashSet<Integer>();
		for(int i = 0;i<al.size();i++)
		{
			reachgrid.add(al.get(i));
		}
		
		int lb_x = (int) ((rect.min_x - total_range.min_x)/resolution);
		int lb_y = (int) ((rect.min_y - total_range.min_y)/resolution);
		int rt_x = (int) ((rect.max_x - total_range.min_x)/resolution);
		int rt_y = (int) ((rect.max_y - total_range.min_y)/resolution);
		
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
		int count = jsonArr.size();

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
			if(jsonObject.has("ReachGrid_"+split_pieces))
			{
				al = new Gson().fromJson(all_attributes.get("ReachGrid_"+split_pieces), listType);
				reachgrid = new HashSet<Integer>();
				for(int j = 0;j<al.size();j++)
				{
					reachgrid.add(al.get(j));
				}
				
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

}
