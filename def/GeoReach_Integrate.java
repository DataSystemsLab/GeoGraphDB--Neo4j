package def;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

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
	public HashMap<Integer, Double> multi_resolution = new HashMap<Integer, Double>();
	public HashMap<Integer, Integer> multi_offset = new HashMap<Integer, Integer>();
	public int level_count;
	
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
		
	private String suffix;
	
	public long neo4j_time;
	public long judge_time;
	
	public int Neo4jAccessCount = 0;
	public int false_inside = 0;
	public int false_outside = 0;
	public int false_all = 0;
	
	public GeoReach_Integrate(MyRectangle rect, int p_split_pieces)
	{
		Config config = new Config();
		suffix = config.GetSuffix();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		RMBR_minx_name = config.GetRMBR_minx_name();
		RMBR_miny_name = config.GetRMBR_miny_name();
		RMBR_maxx_name = config.GetRMBR_maxx_name();
		RMBR_maxy_name = config.GetRMBR_maxy_name();
		
		total_range = new MyRectangle();
		total_range.min_x = rect.min_x;
		total_range.min_y = rect.min_y;
		total_range.max_x = rect.max_x;
		total_range.max_y = rect.max_y;
		
		split_pieces = p_split_pieces;
		
		resolution = (total_range.max_x - total_range.min_x)/split_pieces;
	
		for(int i = 2;i<=split_pieces;i*=2)
		{
			multi_resolution.put(i,(total_range.max_x - total_range.min_x)/(i*i));
		}
		
		int sum = 0;
		for(int i = split_pieces;i>=2;i/=2)
		{
			multi_offset.put(i, sum);
			sum+=i*i;
		}
		level_count = (int)Math.log(split_pieces);
			
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
		neo4j_time = 0;
		judge_time = 0;
	}
	
	public void Preprocess() {
		// TODO Auto-generated method stub
		
	}
	
	public void LoadCompresedBitmap(int split_pieces, String datasource, String filesuffix,int ratio)
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
		int node_count = OwnMethods.GetNodeCount(datasource);
		
//		for(int ratio = 20;ratio<=80;ratio+=20)
		//int ratio = 60;
		{
			long offset = ratio / 20 * node_count;
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+filesuffix+"/Bitmap_"+ratio+".txt");
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split("\t");
					int id = Integer.parseInt(l[0]);
					String bitmap = l[1];
										
					inserter.setNodeProperty(id + offset, "Bitmap_"+split_pieces+suffix, bitmap);
				}
				reader.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
				if(inserter!=null)
					inserter.shutdown();
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

	public void Set_HasBitmap_Boolean_Counting(String datasource,int split_pieces,int threshold, String type)
	{
		long node_count;
		BatchInserter inserter = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
				
		for(int ratio = 20;ratio<=80;ratio+=20)
//		int ratio = 20;
		{
			BufferedReader reader = null;
			File file = null;
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+".txt");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				node_count = Long.parseLong(tempString);
				long offset = ratio / 20 * node_count;
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split("\t");
					int id = Integer.parseInt(l[0]);
					String bitmap = l[1];
					
					ImmutableRoaringBitmap r = OwnMethods.Deserialize_String_ToRoarBitmap(bitmap);
					int count = 0;
					Iterator<Integer> i = r.iterator();
					while(i.hasNext())
					{
						i.next();
						count++;
					}

					if(count<=threshold)
					{
						inserter.setNodeProperty(id + offset, "HasBitmap_"+split_pieces+"_"+threshold+suffix, true);
					}
				}
				reader.close();					
			}
			catch(Exception e)
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
				System.out.println(OwnMethods.ClearCache());
			}
		}
	}
	
	public void Set_HasBitmap_Boolean_Reading(String datasource,int split_pieces, int threshold, String filepath,int ratio)
	{
		long node_count;
		BatchInserter inserter = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
				
//		for(int ratio = 20;ratio<=80;ratio+=20)
//		int ratio = 20;
		{
			BufferedReader reader = null;
			File file = null;
			try
			{
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+filepath+"/Bitmap_"+ratio+"_partial.txt");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				node_count = OwnMethods.GetNodeCount(datasource);
				long offset = ratio / 20 * node_count;
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split("\t");
					int id = Integer.parseInt(l[0]);
					inserter.setNodeProperty(id + offset, "HasBitmap_"+split_pieces+"_"+threshold+suffix, true);
				}
				reader.close();					
			}
			catch(Exception e)
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
				System.out.println(OwnMethods.ClearCache());
			}
		}
	}
	
	private boolean TraversalQuery(int start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
				ArrayList<Integer> al = new Gson().fromJson(jsonObject.get("ReachGrid_"+split_pieces+suffix), listType);
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
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(RMBR_minx_name))
		{
			judge_time += System.currentTimeMillis() - start;
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
		ArrayList<Integer> al = new Gson().fromJson(all_attributes.get("ReachGrid_"+split_pieces+suffix), listType);
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
			false_outside+=1;
			return false;
		}
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				al = new Gson().fromJson(jsonObject.get("ReachGrid_"+split_pieces+suffix), listType);
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
			false_all+=1;
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
		false_inside+=1;
		return false;
	}
	
	private boolean TraversalQuery_FullGrids(int start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			if(jsonObject.has("Bitmap_"+split_pieces+suffix))
			{
				
				String ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
				ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
							
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
			boolean reachable = TraversalQuery_FullGrids(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery_FullGrids(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has("Bitmap_"+split_pieces+suffix))
		{
			judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		//Grid Section
		String ser = all_attributes.get("Bitmap_"+split_pieces+suffix).getAsString();
		ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
		ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
		
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
			false_outside+=1;
			return false;
		}
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
					System.out.println(id);
					return true;
				}
			}
			
			if(jsonObject.has("Bitmap_"+split_pieces+suffix))
			{
				ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
				newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				reachgrid = new ImmutableRoaringBitmap(newbb);
				
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
			false_all+=1;
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
			boolean reachable = TraversalQuery_FullGrids(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
	private boolean TraversalQuery_Bitmap_MultiResolution(int start_id, MyRectangle rect, HashMap<Integer,Integer> lb_x_hash, HashMap<Integer,Integer> lb_y_hash, HashMap<Integer,Integer> rt_x_hash, HashMap<Integer,Integer> rt_y_hash)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				if(jsonObject.has("HasBitmap"+suffix))
				{
					String ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				    ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);

				    int outside_count = 0;
				    for(int level_pieces = 2;level_pieces <= 128;level_pieces*=2)
				    {
				    	int lb_x = lb_x_hash.get(level_pieces);
						int lb_y = lb_y_hash.get(level_pieces);
						int rt_x = rt_x_hash.get(level_pieces);
						int rt_y = rt_y_hash.get(level_pieces);
						
						int offset = multi_offset.get(level_pieces);
						
						//ReachGrid totally Lie In query rectangle
						if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
						{
							for(int k = lb_x+1;k<rt_x;k++)
							{
								for(int j = lb_y+1;j<rt_y;j++)
								{
									int grid_id = k*split_pieces+j;
									if(reachgrid.contains(grid_id+offset))
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
							int grid_id = lb_x*level_pieces+j;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
							grid_id = rt_x*level_pieces+j;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
						}
						for(int j = lb_x+1;j<rt_x;j++)
						{
							int grid_id = j*level_pieces+lb_y;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
							grid_id = j*level_pieces+rt_y;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
						}
						if(flag == false)
						{
							outside_count+=1;
						}
					}
				    if(outside_count == level_count)
				    {
						false_count+=1;
						VisitedVertices.add(id);
				    }
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
			boolean reachable = TraversalQuery_Bitmap_MultiResolution(id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
			
			if(reachable)
				return true;		
		}
		
		return false;
	}
	
	public boolean ReachabilityQuery_Bitmap_MultiResolution(int start_id, MyRectangle rect,int merge_ratio) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(RMBR_minx_name))
		{
			judge_time += System.currentTimeMillis() - start;
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
		
		String ser = null;
		ByteBuffer newbb = null;
		ImmutableRoaringBitmap reachgrid = null;
		
		HashMap<Integer,Integer> lb_x_hash = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> lb_y_hash = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> rt_x_hash = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> rt_y_hash = new HashMap<Integer,Integer>();
		
		for(int level_pieces = 2;level_pieces<=128;level_pieces*=2)
	    {
			int lb_x = (int) ((rect.min_x - total_range.min_x)/multi_resolution.get(level_pieces));
			int lb_y = (int) ((rect.min_y - total_range.min_y)/multi_resolution.get(level_pieces));
			int rt_x = (int) ((rect.max_x - total_range.min_x)/multi_resolution.get(level_pieces));
			int rt_y = (int) ((rect.max_y - total_range.min_y)/multi_resolution.get(level_pieces));
			
			lb_x_hash.put(level_pieces, lb_x);
			lb_y_hash.put(level_pieces, lb_y);
			rt_x_hash.put(level_pieces, rt_x);
			rt_y_hash.put(level_pieces, rt_y);
			
	    }		
		
		//Grid Section
		if(all_attributes.has("HasBitmap"+suffix))
		{
			ser = all_attributes.get("Bitmap_"+split_pieces+"_"+merge_ratio+suffix).getAsString();
			newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
		    reachgrid = new ImmutableRoaringBitmap(newbb);

			int	outside_count = 0;
		    for(int level_pieces = 2;level_pieces <= 128;level_pieces*=2)
		    {
		    	int lb_x = lb_x_hash.get(level_pieces);
				int lb_y = lb_y_hash.get(level_pieces);
				int rt_x = rt_x_hash.get(level_pieces);
				int rt_y = rt_y_hash.get(level_pieces);
				
				int offset = multi_offset.get(level_pieces);
				
				//ReachGrid totally Lie In query rectangle
				if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
				{
					for(int i = lb_x+1;i<rt_x;i++)
					{
						for(int j = lb_y+1;j<rt_y;j++)
						{
							int grid_id = i*level_pieces+j;
							if(reachgrid.contains(grid_id+offset))
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
					int grid_id = lb_x*level_pieces+i;
					if(reachgrid.contains(grid_id+offset))
						flag = true;
					grid_id = rt_x*level_pieces+i;
					if(reachgrid.contains(grid_id+offset))
						flag = true;
				}
				for(int i = lb_x+1;i<rt_x;i++)
				{
					int grid_id = i*level_pieces+lb_y;
					if(reachgrid.contains(grid_id+offset))
						flag = true;
					grid_id = i*level_pieces+rt_y;
					if(reachgrid.contains(grid_id+offset))
						flag = true;
				}
				if(flag == false)
				{
					outside_count++;
				}
		    }
		    if(outside_count == level_count)
		    {
		    	judge_time += System.currentTimeMillis() - start;
				false_outside+=1;
				return false;
		    }
		}
		
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				if(jsonObject.has("HasBitmap"+suffix))
				{
					ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
					reachgrid = new ImmutableRoaringBitmap(newbb);
					
					int outside_count = 0;
					for(int level_pieces = 2;level_pieces<=128;level_pieces*=2)
					{
						int lb_x = lb_x_hash.get(level_pieces);
						int lb_y = lb_y_hash.get(level_pieces);
						int rt_x = rt_x_hash.get(level_pieces);
						int rt_y = rt_y_hash.get(level_pieces);
						
						int offset = multi_offset.get(level_pieces);
						
						//ReachGrid totally Lie In query rectangle
						if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
						{
							for(int k = lb_x+1;k<rt_x;k++)
							{
								for(int j = lb_y+1;j<rt_y;j++)
								{
									int grid_id = k*level_pieces+j;
									if(reachgrid.contains(grid_id+offset))
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
							int grid_id = lb_x*level_pieces+j;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
							grid_id = rt_x*level_pieces+j;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
						}
						for(int j = lb_x+1;j<rt_x;j++)
						{
							int grid_id = j*level_pieces+lb_y;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
							grid_id = j*level_pieces+rt_y;
							if(reachgrid.contains(grid_id+offset))
								flag = true;
						}
						if(flag == false)
						{
							outside_count++;
						}					
					}
					if(outside_count == level_count)
					{
						false_count+=1;
						VisitedVertices.add(id);
					}
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
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_MultiResolution(id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
	private boolean TraversalQuery_Bitmap_Partial(int start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				if(jsonObject.has("HasBitmap_"+split_pieces+"_"+200+suffix))
				{
					String ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				    ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);

					
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
			boolean reachable = TraversalQuery_Bitmap_Partial(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery_Bitmap_Partial(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(RMBR_minx_name))
		{
			judge_time += System.currentTimeMillis() - start;
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
		
		String ser = null;
		ByteBuffer newbb = null;
		ImmutableRoaringBitmap reachgrid = null;
		
		int lb_x = (int) ((rect.min_x - total_range.min_x)/resolution);
		int lb_y = (int) ((rect.min_y - total_range.min_y)/resolution);
		int rt_x = (int) ((rect.max_x - total_range.min_x)/resolution);
		int rt_y = (int) ((rect.max_y - total_range.min_y)/resolution);
		
		boolean flag = false;
		
		//Grid Section
		if(all_attributes.has("HasBitmap_"+split_pieces+"_"+200+suffix))
		{
			ser = all_attributes.get("Bitmap_"+split_pieces+suffix).getAsString();
			newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
		    reachgrid = new ImmutableRoaringBitmap(newbb);
			
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
			flag = false;
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
				false_outside+=1;
				return false;
			}
		}
		
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
				if(jsonObject.has("HasBitmap_"+split_pieces+"_"+200+suffix))
				{
					ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
					reachgrid = new ImmutableRoaringBitmap(newbb);
					
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
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_Partial(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
	private boolean TraversalQuery_Bitmap_Total(int start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		
		long start = System.currentTimeMillis();
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
//				if(jsonObject.has("HasBitmap"+suffix))
				{
					String ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				    ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);

					
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
			boolean reachable = TraversalQuery_Bitmap_Total(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	public boolean ReachabilityQuery_Bitmap_Total(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(RMBR_minx_name))
		{
			judge_time += System.currentTimeMillis() - start;
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
		
		String ser = null;
		ByteBuffer newbb = null;
		ImmutableRoaringBitmap reachgrid = null;
		
		int lb_x = (int) ((rect.min_x - total_range.min_x)/resolution);
		int lb_y = (int) ((rect.min_y - total_range.min_y)/resolution);
		int rt_x = (int) ((rect.max_x - total_range.min_x)/resolution);
		int rt_y = (int) ((rect.max_y - total_range.min_y)/resolution);
		
		boolean flag = false;
		
		//Grid Section
//		if(all_attributes.has("HasBitmap"+suffix))
		{
			ser = all_attributes.get("Bitmap_"+split_pieces+suffix).getAsString();
			newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
		    reachgrid = new ImmutableRoaringBitmap(newbb);
			
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
			flag = false;
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
				false_outside+=1;
				return false;
			}
		}
		
		
		judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
					judge_time += System.currentTimeMillis() - start;
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
					judge_time += System.currentTimeMillis() - start;
					return true;
				}
				if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				
//				if(jsonObject.has("HasBitmap"+suffix))
				{
					ser = jsonObject.get("Bitmap_"+split_pieces+suffix).getAsString();
					newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
					reachgrid = new ImmutableRoaringBitmap(newbb);
					
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
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_Total(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
}
