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
import com.google.gson.JsonPrimitive;
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
	
	private String bitmap_name;
		
	private String suffix;
	
	public long query_neo4j_time;
	public long query_judge_time;
	public long judge_1_time;
	public long judge_2_time;
	
	public int Neo4jAccessCount = 0;
	public int false_inside = 0;
	public int false_outside = 0;
	public int false_all = 0;
	
	public long update_addedge_time;
	public long update_deleteedge_time;
	
	public long update_neo4j_time;
	public long update_inmemory_time;
	
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
		bitmap_name = "Bitmap_"+split_pieces+suffix;
		
		total_range = new MyRectangle();
		total_range.min_x = rect.min_x;
		total_range.min_y = rect.min_y;
		total_range.max_x = rect.max_x;
		total_range.max_y = rect.max_y;
		
		split_pieces = p_split_pieces;
		
		resolution = (total_range.max_x - total_range.min_x)/split_pieces;
	
		for(int i = 2;i<=split_pieces;i*=2)
		{
			multi_resolution.put(i,(total_range.max_x - total_range.min_x)/(i));
		}
		
		int sum = 0;
		for(int i = split_pieces;i>=2;i/=2)
		{
			multi_offset.put(i, sum);
			sum+=i*i;
		}
		level_count = (int)(Math.log(split_pieces)/(Math.log(2.0)));
			
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();
		query_neo4j_time = 0;
		query_judge_time = 0;
		judge_1_time = 0;
		judge_2_time = 0;
		
		update_addedge_time = 0;
		update_deleteedge_time = 0;
		update_neo4j_time = 0;
		update_inmemory_time = 0;
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
	
	public void LoadPartialCompresedBitmap(int split_pieces, String datasource, String filesuffix,int ratio, int threshold)
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
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+filesuffix+"/Bitmap_"+ratio+"_partial.txt");
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
					inserter.setNodeProperty(id+offset, "HasBitmap_"+split_pieces+"_"+threshold+suffix, true);
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
	
	public void LoadMultilevelBitmap(int split_pieces, String datasource, String filesuffix,int ratio, int merge_count)
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
		int node_count = OwnMethods.GetNodeCount(datasource);
		int id = 0;
		
//		for(int ratio = 20;ratio<=80;ratio+=20)
		//int ratio = 60;
		{
			String tempString = null;
			long offset = ratio / 20 * node_count;
			try
			{
				String filename = String.format("/home/yuhansun/Documents/Real_data/%s/GeoReachGrid_%d/%s/Bitmap_%d_multilevelfull_%d.txt", datasource, split_pieces, filesuffix, ratio,merge_count);
				file = new File(filename);
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				tempString = null;
				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split("\t");
					id = Integer.parseInt(l[0]);
					String bitmap = l[1];
										
					inserter.setNodeProperty(id + offset, "MultilevelBitmap_"+split_pieces+"_"+merge_count+suffix, bitmap);
				}
				reader.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				if(inserter!=null)
					inserter.shutdown();
				System.out.println(id);
				System.out.println(tempString);
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
	
	public void Set_HasBitmap_Boolean_Reading(String datasource,int split_pieces, int threshold, String filesuffix,int ratio)
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
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+filesuffix+"/Bitmap_"+ratio+"_partial.txt");
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
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
								query_judge_time += System.currentTimeMillis() - start;
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
			boolean reachable = TraversalQuery(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	//RMBR and full grid list
	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
		
		//RMBR No overlap case
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		//RMBR Contain case
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
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
						query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_outside+=1;
			return false;
		}
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
								query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_all+=1;
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
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
					query_judge_time += System.currentTimeMillis() - start;
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
								query_judge_time += System.currentTimeMillis() - start;
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
			boolean reachable = TraversalQuery_FullGrids(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	//grids bitmap
	public boolean ReachabilityQuery_FullGrids(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has("Bitmap_"+split_pieces+suffix))
		{
			query_judge_time += System.currentTimeMillis() - start;
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
						query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_outside+=1;
			return false;
		}
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
					query_judge_time += System.currentTimeMillis() - start;
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
								query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_all+=1;
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
			boolean reachable = TraversalQuery_FullGrids(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
	private boolean TraversalQuery_Bitmap_MultiResolution(int start_id, MyRectangle rect, int merge_ratio, HashMap<Integer,Integer> lb_x_hash, HashMap<Integer,Integer> lb_y_hash, HashMap<Integer,Integer> rt_x_hash, HashMap<Integer,Integer> rt_y_hash)
	{	
		String bitmap_name = String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix);
		
		long start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Long.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		start = System.currentTimeMillis();
		int false_count = 0;
		for(int json_index = 0;json_index<jsonArr.size();json_index++)
		{			
			jsonObject = (JsonObject)jsonArr.get(json_index);
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
			
			if(jsonObject.has(String.format(bitmap_name)))
			{
				String ser = jsonObject.get(String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix)).getAsString();
				ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
				ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
				
				int outside_count = 0;
				for(int level_pieces = split_pieces;level_pieces >=2;level_pieces/=2)
				{
					int lb_x = lb_x_hash.get(level_pieces);
					int lb_y = lb_y_hash.get(level_pieces);
					int rt_x = rt_x_hash.get(level_pieces);
					int rt_y = rt_y_hash.get(level_pieces);
					
					int offset = multi_offset.get(level_pieces);
//					int query_rec_grid_count = (rt_y - lb_y)*(rt_x - lb_y);
//					int card = reachgrid.getCardinality();
//					if(card>query_rec_grid_count)
//					{
//						long p_start = System.currentTimeMillis();
//						//ReachGrid totally Lie In query rectangle
//						if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
//						{
//							for(int i = lb_x+1;i<rt_x;i++)
//							{
//								for(int j = lb_y+1;j<rt_y;j++)
//								{
//									int grid_id = i*level_pieces+j;
//									if(reachgrid.contains(grid_id+offset))
//									{
//										judge_1_time+=System.currentTimeMillis() - p_start;
//										judge_time += System.currentTimeMillis() - start;
//										return true;
//									}
//								}
//							}
//						}
//
//						//ReachGrid No overlap with query rectangle
//						boolean flag = false;
//						for(int i = lb_y;i<=rt_y;i++)
//						{
//							int grid_id = lb_x*level_pieces+i;
//							if(reachgrid.contains(grid_id+offset))
//								flag = true;
//							grid_id = rt_x*level_pieces+i;
//							if(reachgrid.contains(grid_id+offset))
//								flag = true;
//						}
//						for(int i = lb_x+1;i<rt_x;i++)
//						{
//							int grid_id = i*level_pieces+lb_y;
//							if(reachgrid.contains(grid_id+offset))
//								flag = true;
//							grid_id = i*level_pieces+rt_y;
//							if(reachgrid.contains(grid_id+offset))
//								flag = true;
//						}
//						judge_1_time+=System.currentTimeMillis() - p_start;
//						if(flag == false)
//						{
//							outside_count++;
//						}
//						else
//							break;
//					}
//					else
					{
						long p_start = System.currentTimeMillis();
						Iterator<Integer> iter = reachgrid.iterator();
						boolean flag = false;
						while(iter.hasNext())
						{
							int grid_id = iter.next() - offset;
							int row_index = (int) (grid_id/level_pieces);
							int col_index = (grid_id - row_index*level_pieces);
							//ReachGrid totally Lie In query rectangle
							if(row_index<rt_x&&row_index>lb_x&&col_index<rt_y&&col_index>lb_y)
							{
								judge_2_time+=System.currentTimeMillis() - p_start;
								query_judge_time += System.currentTimeMillis() - start;
								return true;
							}
							
							//ReachGrid No overlap with query rectangle
							
							if(row_index>rt_x||row_index<lb_x||col_index>rt_y||col_index<lb_y)
							{
								flag = false;
							}
							else
							{
								flag = true;
								break;
							}
						}
						judge_2_time+=System.currentTimeMillis() - p_start;
						if(flag == false)
						{
							outside_count++;
						}
						else
							break;
					}					
				}
				if(outside_count == level_count)
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
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_MultiResolution(id, rect, merge_ratio, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}


	
	public boolean ReachabilityQuery_Bitmap_MultiResolution(long start_id, MyRectangle rect, int merge_ratio) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		start = System.currentTimeMillis();
		
		if(!all_attributes.has(String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix)))
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
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
//		if(all_attributes.has("HasBitmap"+suffix))
		{
			ser = all_attributes.get(String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix)).getAsString();
			newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
		    reachgrid = new ImmutableRoaringBitmap(newbb);

			int	outside_count = 0;
		    for(int level_pieces = split_pieces;level_pieces >=2;level_pieces/=2)
		    {
		    	int lb_x = lb_x_hash.get(level_pieces);
				int lb_y = lb_y_hash.get(level_pieces);
				int rt_x = rt_x_hash.get(level_pieces);
				int rt_y = rt_y_hash.get(level_pieces);
				
				int offset = multi_offset.get(level_pieces);
//				int query_rec_grid_count = (rt_y - lb_y)*(rt_x - lb_y);
//				int card = reachgrid.getCardinality();
//				if(card>query_rec_grid_count)
//				{
//					long p_start = System.currentTimeMillis();
//					//ReachGrid totally Lie In query rectangle
//					if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
//					{
//						for(int i = lb_x+1;i<rt_x;i++)
//						{
//							for(int j = lb_y+1;j<rt_y;j++)
//							{
//								int grid_id = i*level_pieces+j;
//								if(reachgrid.contains(grid_id+offset))
//								{
//									judge_1_time+=System.currentTimeMillis() - p_start;
//									judge_time += System.currentTimeMillis() - start;
//									return true;
//								}
//							}
//						}
//					}
//
//					//ReachGrid No overlap with query rectangle
//					boolean flag = false;
//					for(int i = lb_y;i<=rt_y;i++)
//					{
//						int grid_id = lb_x*level_pieces+i;
//						if(reachgrid.contains(grid_id+offset))
//							flag = true;
//						grid_id = rt_x*level_pieces+i;
//						if(reachgrid.contains(grid_id+offset))
//							flag = true;
//					}
//					for(int i = lb_x+1;i<rt_x;i++)
//					{
//						int grid_id = i*level_pieces+lb_y;
//						if(reachgrid.contains(grid_id+offset))
//							flag = true;
//						grid_id = i*level_pieces+rt_y;
//						if(reachgrid.contains(grid_id+offset))
//							flag = true;
//					}
//					judge_1_time+=System.currentTimeMillis() - p_start;
//					if(flag == false)
//					{
//						outside_count++;
//					}
//					else
//						break;
//				}
//				else
				{
					long p_start = System.currentTimeMillis();
					Iterator<Integer> iter = reachgrid.iterator();
					boolean flag = false;
					while(iter.hasNext())
					{
						int grid_id = iter.next() - offset;
						int row_index = (int) (grid_id/level_pieces);
						int col_index = (grid_id - row_index*level_pieces);
						//ReachGrid totally Lie In query rectangle
						if(row_index<rt_x&&row_index>lb_x&&col_index<rt_y&&col_index>lb_y)
						{
							query_judge_time += System.currentTimeMillis() - start;
							judge_2_time+=System.currentTimeMillis() -  p_start;
							return true;
						}
						
						//ReachGrid No overlap with query rectangle
						
						if(row_index>rt_x||row_index<lb_x||col_index>rt_y||col_index<lb_y)
						{
							flag = false;
						}
						else
						{
							flag = true;
							break;
						}
					}
					judge_2_time+=System.currentTimeMillis() -  p_start;
					if(flag == false)
					{
						outside_count++;
					}
					else
						break;
				}
		    }
		    if(outside_count == level_count)
		    {
		    	query_judge_time += System.currentTimeMillis() - start;
				false_outside+=1;
				return false;
		    }
		}
		
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Long.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		start = System.currentTimeMillis();
		int false_count = 0;
		for(int json_index = 0;json_index<jsonArr.size();json_index++)
		{			
			jsonObject = (JsonObject)jsonArr.get(json_index);
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
			
			if(jsonObject.has(String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix)))
			{
//				if(jsonObject.has("HasBitmap"+suffix))
				{
					ser = jsonObject.get(String.format("MultilevelBitmap_%d_%d%s", split_pieces, merge_ratio, suffix)).getAsString();
					newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
					reachgrid = new ImmutableRoaringBitmap(newbb);
					
					int outside_count = 0;
					for(int level_pieces = split_pieces;level_pieces >=2;level_pieces/=2)
					{
						int lb_x = lb_x_hash.get(level_pieces);
						int lb_y = lb_y_hash.get(level_pieces);
						int rt_x = rt_x_hash.get(level_pieces);
						int rt_y = rt_y_hash.get(level_pieces);
						
						int offset = multi_offset.get(level_pieces);
//						int query_rec_grid_count = (rt_y - lb_y)*(rt_x - lb_y);
//						int card = reachgrid.getCardinality();
////						if(card>query_rec_grid_count)
////						{
//							long p_start = System.currentTimeMillis();
//							//ReachGrid totally Lie In query rectangle
//							if((rt_x-lb_x>1)&&(rt_y-lb_y)>1)
//							{
//								for(int i = lb_x+1;i<rt_x;i++)
//								{
//									for(int j = lb_y+1;j<rt_y;j++)
//									{
//										int grid_id = i*level_pieces+j;
//										if(reachgrid.contains(grid_id+offset))
//										{
//											judge_1_time += System.currentTimeMillis() - p_start;
//											judge_time += System.currentTimeMillis() - start;
//											return true;
//										}
//									}
//								}
//							}
//
//							//ReachGrid No overlap with query rectangle
//							boolean flag = false;
//							for(int i = lb_y;i<=rt_y;i++)
//							{
//								int grid_id = lb_x*level_pieces+i;
//								if(reachgrid.contains(grid_id+offset))
//									flag = true;
//								grid_id = rt_x*level_pieces+i;
//								if(reachgrid.contains(grid_id+offset))
//									flag = true;
//							}
//							for(int i = lb_x+1;i<rt_x;i++)
//							{
//								int grid_id = i*level_pieces+lb_y;
//								if(reachgrid.contains(grid_id+offset))
//									flag = true;
//								grid_id = i*level_pieces+rt_y;
//								if(reachgrid.contains(grid_id+offset))
//									flag = true;
//							}
//							judge_1_time += System.currentTimeMillis() - p_start;
//							if(flag == false)
//							{
//								outside_count++;
//							}
//							else
//								break;
//						}
//						else
						{
							long p_start = System.currentTimeMillis();
							Iterator<Integer> iter = reachgrid.iterator();
							boolean flag = false;
							while(iter.hasNext())
							{
								int grid_id = iter.next() - offset;
								int row_index = (int) (grid_id/level_pieces);
								int col_index = (grid_id - row_index*level_pieces);
								//ReachGrid totally Lie In query rectangle
								if(row_index<rt_x&&row_index>lb_x&&col_index<rt_y&&col_index>lb_y)
								{
									judge_2_time+=System.currentTimeMillis() - p_start;
									query_judge_time += System.currentTimeMillis() - start;
									return true;
								}
								
								//ReachGrid No overlap with query rectangle
								if(row_index>rt_x||row_index<lb_x||col_index>rt_y||col_index<lb_y)
								{
									flag = false;
								}
								else
								{
									flag = true;
									break;
								}
							}
							judge_2_time+=System.currentTimeMillis() - p_start;

							if(flag == false)
							{
								outside_count++;
							}
							else
								break;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_MultiResolution(id, rect, merge_ratio, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
			
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
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
									query_judge_time += System.currentTimeMillis() - start;
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
			boolean reachable = TraversalQuery_Bitmap_Partial(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	//RMBR and partial grids bitmap
	public boolean ReachabilityQuery_Bitmap_Partial(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
		
		//RMBR No overlap case
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		//RMBR Contain case
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
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
							query_judge_time += System.currentTimeMillis() - start;
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
				query_judge_time += System.currentTimeMillis() - start;
				false_outside+=1;
				return false;
			}
		}
		
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
									query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_all+=1;
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
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
									query_judge_time += System.currentTimeMillis() - start;
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
			boolean reachable = TraversalQuery_Bitmap_Total(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;		
		}
		
		return false;	
	}
	
	//RMBR and grids bitmap
	public boolean ReachabilityQuery_Bitmap_Total(int start_id, MyRectangle rect) 
	{
		VisitedVertices.clear();
		long start = System.currentTimeMillis();
		JsonObject all_attributes = p_neo4j_graph_store.GetVertexAllAttributes(start_id);
		query_neo4j_time += System.currentTimeMillis() - start;
		
		Neo4jAccessCount+=1;
		
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
		
		//RMBR No overlap case
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
			return false;
		}
		
		//RMBR Contain case
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
		{
			query_judge_time += System.currentTimeMillis() - start;
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
							query_judge_time += System.currentTimeMillis() - start;
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
				query_judge_time += System.currentTimeMillis() - start;
				false_outside+=1;
				return false;
			}
		}
		
		
		query_judge_time += System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		String query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b), b";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		query_neo4j_time += System.currentTimeMillis() - start;
		
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
									query_judge_time += System.currentTimeMillis() - start;
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
			query_judge_time += System.currentTimeMillis() - start;
			false_all+=1;
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
			boolean reachable = TraversalQuery_Bitmap_Total(id, rect, lb_x, lb_y, rt_x, rt_y);
			
			if(reachable)
				return true;
			
		}
		false_inside+=1;
		return false;
	}
	
	//used in UpdateAddEdge to recursively update changing of RMBR(the end_id must be changed if the function is called and jArr_end store end_id's location and RMBR in a jsonArray)
	public void UpdateAddEdgeTraverse(long end_id, JsonArray jArr_end)
	{
		String query = null;
		String result = null;
				
		long start = System.currentTimeMillis();
		query = String.format("match (n)-->(b) where id(b) = %d return n.%s, n.%s, n.%s, id(n)", end_id, longitude_property_name, latitude_property_name, bitmap_name);
		result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time+=System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		for(int j_index = 0;j_index<jsonArr.size();j_index++)
		{
			JsonArray jArr_start = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
			long start_id = jArr_start.get(3).getAsLong();
			boolean flag = false;
			
			RoaringBitmap update_bitmap = null;
			//start node has no bitmap
			if(jArr_start.get(2).isJsonNull())
			{
				//end node has location
				if(!jArr_end.get(0).isJsonNull())
				{
					flag = true;
					double lon = jArr_end.get(0).getAsDouble();
					double lat = jArr_end.get(1).getAsDouble();
					
					int grid_x = (int) ((lon - total_range.min_x)/resolution);
					int grid_y = (int) ((lat - total_range.min_y)/resolution);
					
					int grid_id = grid_x*split_pieces+grid_y;
					update_bitmap = new RoaringBitmap();
					update_bitmap.add(grid_id);
					
					//end node has bitmap
					if(!jArr_end.get(2).isJsonNull())
					{
						ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).toString());
						Iterator<Integer> iter = end_bitmap.iterator();
						while(iter.hasNext())
							update_bitmap.add(iter.next());	
					}
				}
				//end node has no location
				else
				{
					//end node has bitmap
					if(!jArr_end.get(2).isJsonNull())
					{
						flag = true;
						update_bitmap = new RoaringBitmap();
						ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).toString());
						Iterator<Integer> iter = end_bitmap.iterator();
						while(iter.hasNext())
							update_bitmap.add(iter.next());	
					}
				}
			}
			//start node has bitmap
			else
			{
				ImmutableRoaringBitmap start_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_start.get(2).getAsString());
				update_bitmap = new RoaringBitmap();
				Iterator<Integer> iter = start_bitmap.iterator();
				while(iter.hasNext())
					update_bitmap.add(iter.next());
				if(!jArr_end.get(0).isJsonNull())
				{
					double lon = jArr_end.get(0).getAsDouble();
					double lat = jArr_end.get(1).getAsDouble();
					
					int grid_x = (int) ((lon - total_range.min_x)/resolution);
					int grid_y = (int) ((lat - total_range.min_y)/resolution);
					
					int grid_id = grid_x*split_pieces+grid_y;
					if(update_bitmap.checkedAdd(grid_id))
						flag = true;
				}
				if(!jArr_end.get(2).isJsonNull())
				{
					ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).getAsString());
					iter = end_bitmap.iterator();
					while(iter.hasNext())
						if(update_bitmap.checkedAdd(iter.next()))
							flag = true;
				}
			}
			update_inmemory_time+=System.currentTimeMillis() - start;
			
			if(flag)
			{
				start = System.currentTimeMillis();
				String bitmap = OwnMethods.Serialize_RoarBitmap_ToString(update_bitmap);
				update_inmemory_time += System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				query = String.format("match (n) where id(n) = %d set n.%s = %s", start_id, bitmap_name, bitmap);
				Neo4j_Graph_Store.Execute(resource, query);
				update_neo4j_time += System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				jArr_start.set(2, new JsonPrimitive(bitmap));
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
		query = String.format("match (n) where id(n) in [%d,%d] return n.%s, n.%s, n.%s", start_id, end_id, longitude_property_name, latitude_property_name, bitmap_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		update_neo4j_time+=System.currentTimeMillis() - start;
		
		start = System.currentTimeMillis();
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		JsonArray jArr_start = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
		JsonArray jArr_end = jsonArr.get(1).getAsJsonObject().get("row").getAsJsonArray();
		
		boolean flag = false;
		
		RoaringBitmap update_bitmap = null;
		//start node has no bitmap
		if(jArr_start.get(2).isJsonNull())
		{
			//end node has location
			if(!jArr_end.get(0).isJsonNull())
			{
				flag = true;
				double lon = jArr_end.get(0).getAsDouble();
				double lat = jArr_end.get(1).getAsDouble();
				
				int grid_x = (int) ((lon - total_range.min_x)/resolution);
				int grid_y = (int) ((lat - total_range.min_y)/resolution);
				
				int grid_id = grid_x*split_pieces+grid_y;
				update_bitmap = new RoaringBitmap();
				update_bitmap.add(grid_id);
								
				//end node has bitmap
				if(!jArr_end.get(2).isJsonNull())
				{
					ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).toString());
					Iterator<Integer> iter = end_bitmap.iterator();
					while(iter.hasNext())
						update_bitmap.add(iter.next());	
				}
			}
			//end node has no location
			else
			{
				//end node has RMBR
				if(!jArr_end.get(2).isJsonNull())
				{
					flag = true;
					update_bitmap = new RoaringBitmap();
					ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).toString());
					Iterator<Integer> iter = end_bitmap.iterator();
					while(iter.hasNext())
						update_bitmap.add(iter.next());	
				}
			}
		}
		//start node has bitmap
		else
		{
			ImmutableRoaringBitmap start_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_start.get(2).getAsString());
			update_bitmap = new RoaringBitmap();
			Iterator<Integer> iter = start_bitmap.iterator();
			while(iter.hasNext())
				update_bitmap.add(iter.next());
			
			if(!jArr_end.get(0).isJsonNull())
			{
				double lon = jArr_end.get(0).getAsDouble();
				double lat = jArr_end.get(1).getAsDouble();
				
				int grid_x = (int) ((lon - total_range.min_x)/resolution);
				int grid_y = (int) ((lat - total_range.min_y)/resolution);
				
				int grid_id = grid_x*split_pieces+grid_y;
				if(update_bitmap.checkedAdd(grid_id))
					flag = true;
			}
			if(!jArr_end.get(2).isJsonNull())
			{
				ImmutableRoaringBitmap end_bitmap = OwnMethods.Deserialize_String_ToRoarBitmap(jArr_end.get(2).getAsString());
				iter = end_bitmap.iterator();
				while(iter.hasNext())
					if(update_bitmap.checkedAdd(iter.next()))
						flag = true;
			}
		}
		
		update_inmemory_time+=System.currentTimeMillis() - start;
		if(flag)
		{
			start = System.currentTimeMillis();
			String bitmap = OwnMethods.Serialize_RoarBitmap_ToString(update_bitmap);
			update_inmemory_time += System.currentTimeMillis() - start;
			
			start = System.currentTimeMillis();
			query = String.format("match (n) where id(n) = %d set n.%s = %s", start_id, bitmap_name, bitmap);
			Neo4j_Graph_Store.Execute(resource, query);
			update_neo4j_time+=System.currentTimeMillis()-start;
			
			start = System.currentTimeMillis();
			jArr_start.set(2, new JsonPrimitive(bitmap));
			update_inmemory_time+=System.currentTimeMillis() - start;
			UpdateAddEdgeTraverse(start_id, jArr_start);			
		}
		return flag;
	}
}
