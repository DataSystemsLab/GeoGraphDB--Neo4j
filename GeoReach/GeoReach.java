package def;

import java.nio.ByteBuffer;
import java.util.*;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
	
	private String GeoB_name;
	private String bitmap_name;
	
	public MyRectangle total_range;
	public int split_pieces;
	public double resolution_x;
	public double resolution_y;
	public HashMap<Integer, Double> multi_resolution_x = new HashMap<Integer, Double>();
	public HashMap<Integer, Double> multi_resolution_y = new HashMap<Integer, Double>();
	public HashMap<Integer, Integer> multi_offset = new HashMap<Integer, Integer>();
	public int level_count;
	
	private int merge_ratio;
	
	public GeoReach(MyRectangle rect, int p_split_pieces)
	{
		Config config = new Config();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		RMBR_minx_name = config.GetRMBR_minx_name();
		RMBR_miny_name = config.GetRMBR_miny_name();
		RMBR_maxx_name = config.GetRMBR_maxx_name();
		RMBR_maxy_name = config.GetRMBR_maxy_name();
		merge_ratio = config.GetMergeRatio();
			
		total_range = new MyRectangle();
		total_range.min_x = rect.min_x;
		total_range.min_y = rect.min_y;
		total_range.max_x = rect.max_x;
		total_range.max_y = rect.max_y;
		
		split_pieces = p_split_pieces;
		GeoB_name = config.GetGeoB_name();
		bitmap_name = config.GetBitmap_name();
		
		resolution_x = (total_range.max_x - total_range.min_x)/split_pieces;
		resolution_y = (total_range.max_y - total_range.min_y)/split_pieces;
	
		for(int i = 2;i<=split_pieces;i*=2)
		{
			multi_resolution_x.put(i,(total_range.max_x - total_range.min_x)/(i));
			multi_resolution_y.put(i,(total_range.max_y - total_range.min_y)/(i));
		}
		
		int sum = 0;
		for(int i = split_pieces;i>=2;i/=2)
		{
			multi_offset.put(i, sum);
			sum+=i*i;
		}
		level_count = (int)((Math.log(split_pieces))/(Math.log(2.0)));
			
		p_neo4j_graph_store = new Neo4j_Graph_Store();
		resource = p_neo4j_graph_store.GetCypherResource();	
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
	
	private boolean TraverseQuery_MT0(long start_id, MyRectangle rect, int lb_x, int lb_y, int rt_x, int rt_y)
	{		
		String query = String.format("match (a)-->(b) where id(a) = %d return id(b), b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, b.%s",  start_id,  GeoB_name, bitmap_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name,RMBR_maxy_name, longitude_property_name, latitude_property_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			//already visited
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}

			else
			{
				if(!row.get(7).isJsonNull())
				{
					double lon = row.get(7).getAsDouble();
					double lat = row.get(8).getAsDouble();
					if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
					{
						//spatial vertex and located in query rectangle
						return true;
					}
				}
				//cannot reach any spatial vertex
				if(!row.get(1).isJsonNull()&&!row.get(2).isJsonNull()&&!row.get(3).isJsonNull())
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				else
				{
					//G-vertex
					if(!row.get(2).isJsonNull())
					{
						String ser = row.get(2).getAsString();
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
						//R-vertex
						if(!row.get(3).isJsonNull())
						{
							MyRectangle RMBR = new MyRectangle(row.get(3).getAsDouble(), row.get(4).getAsDouble(), row.get(5).getAsDouble(), row.get(6).getAsDouble());
							
							if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
							{
								return true;
							}
							
							if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
							{
								false_count+=1;
								VisitedVertices.add(id);
								continue;
							}	
						}
					}
				
				}	
			}
		}
	
		//all out-neighbors are impossible to reach query rectangle
		if(false_count == jsonArr.size()) 
		{
			return false;
		}
		else
		{
			for(int i = 0;i<jsonArr.size();i++)
			{
				JsonObject jsonObject = (JsonObject)jsonArr.get(i);
				JsonArray row = (JsonArray)jsonObject.get("row");
				
				int id = row.get(0).getAsInt();
				if(VisitedVertices.contains(id))
				{
					continue;
				}
				VisitedVertices.add(id);
				boolean reachable = TraverseQuery_MT0(id, rect, lb_x, lb_y, rt_x, rt_y);
				
				if(reachable)
					return true;		
			}
			return false;	
		}
	}
	
	public boolean ReachabilityQuery_MT0(long start_id, MyRectangle rect)
	{
		VisitedVertices.clear();
		String query = String.format("match (n) where id(n) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", start_id,  GeoB_name, bitmap_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name,RMBR_maxy_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		jsonArr = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
		
		//start_id vertex cannot reach any spatial vertices
		if(jsonArr.get(0).isJsonNull()&&jsonArr.get(1).isJsonNull()&&jsonArr.get(2).isJsonNull())
		{
			return false;
		}
		
		else
		{
			int lb_x = (int) ((rect.min_x - total_range.min_x)/resolution_x);
			int lb_y = (int) ((rect.min_y - total_range.min_y)/resolution_y);
			int rt_x = (int) ((rect.max_x - total_range.min_x)/resolution_x);
			int rt_y = (int) ((rect.max_y - total_range.min_y)/resolution_y);
			
			//G-vertex
			if(!jsonArr.get(1).isJsonNull())
			{
				String ser = null;
				ByteBuffer newbb = null;
				ImmutableRoaringBitmap reachgrid = null;
				
				boolean flag = false;
				ser = jsonArr.get(1).getAsString();
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
								return true;
							}
						}
					}
				}

				//ReachGrid No overlap with query rectangle
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
					return false;
				}
				return TraverseQuery_MT0(start_id, rect, lb_x, lb_y, rt_x, rt_y);
			}
			else
			{
				//R-vertex
				if(!jsonArr.get(2).isJsonNull())
				{
					MyRectangle RMBR = new MyRectangle(jsonArr.get(2).getAsDouble(), jsonArr.get(3).getAsDouble(), jsonArr.get(4).getAsDouble(), jsonArr.get(5).getAsDouble());
					
					//RMBR No overlap case
					if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
					{
						return false;
					}
					
					//RMBR Contain case
					if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
					{
						return true;
					}
					
					return TraverseQuery_MT0(start_id, rect, lb_x, lb_y, rt_x, rt_y);
				}
				
				else
				{
					//B-vertex
					return TraverseQuery_MT0(start_id, rect, lb_x, lb_y, rt_x, rt_y);
				}
			}
		}
	}
	
	private boolean TraverseQuery(long start_id, MyRectangle rect, HashMap<Integer,Integer> lb_x_hash, HashMap<Integer,Integer> lb_y_hash, HashMap<Integer,Integer> rt_x_hash, HashMap<Integer,Integer> rt_y_hash)
	{		
		String query = String.format("match (a)-->(b) where id(a) = %d return id(b), b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, b.%s, b.%s",  start_id,  GeoB_name, bitmap_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name,RMBR_maxy_name, longitude_property_name, latitude_property_name);
		String result = Neo4j_Graph_Store.Execute(resource, query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		int false_count = 0;
		for(int i = 0;i<jsonArr.size();i++)
		{			
			JsonObject jsonObject = (JsonObject)jsonArr.get(i);
			JsonArray row = (JsonArray)jsonObject.get("row");
			
			int id = row.get(0).getAsInt();
			//already visited
			if(VisitedVertices.contains(id))
			{
				false_count+=1;
				continue;
			}

			else
			{
				if(!row.get(7).isJsonNull())
				{
					double lon = row.get(7).getAsDouble();
					double lat = row.get(8).getAsDouble();
					if(Neo4j_Graph_Store.Location_In_Rect(lat, lon, rect))
					{
						//spatial vertex and located in query rectangle
						return true;
					}
				}
				//cannot reach any spatial vertex
				if(!row.get(1).isJsonNull()&&!row.get(2).isJsonNull()&&!row.get(3).isJsonNull())
				{
					false_count+=1;
					VisitedVertices.add(id);
					continue;
				}
				else
				{
					//G-vertex
					if(!row.get(2).isJsonNull())
					{
						String ser = row.get(2).getAsString();
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
							if(flag == false)
							{
								outside_count++;
							}
							else
								break;
						}
						if(outside_count == level_count)
						{
							false_count+=1;
							VisitedVertices.add(id);
						}
					}
					else
					{
						//R-vertex
						if(!row.get(3).isJsonNull())
						{
							MyRectangle RMBR = new MyRectangle(row.get(3).getAsDouble(), row.get(4).getAsDouble(), row.get(5).getAsDouble(), row.get(6).getAsDouble());
							
							if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
							{
								return true;
							}
							
							if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
							{
								false_count+=1;
								VisitedVertices.add(id);
								continue;
							}	
						}
					}
				
				}	
			}
		}
	
		//all out-neighbors are impossible to reach query rectangle
		if(false_count == jsonArr.size()) 
		{
			return false;
		}
		else
		{
			for(int i = 0;i<jsonArr.size();i++)
			{
				JsonObject jsonObject = (JsonObject)jsonArr.get(i);
				JsonArray row = (JsonArray)jsonObject.get("row");
				
				int id = row.get(0).getAsInt();
				if(VisitedVertices.contains(id))
				{
					continue;
				}
				VisitedVertices.add(id);
				boolean reachable = TraverseQuery(id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
				
				if(reachable)
					return true;		
			}
			return false;	
		}
	}
	
	public boolean ReachabilityQuery(long start_id, MyRectangle rect)
	{
		VisitedVertices.clear();
		//no merge and only one layer
		if(merge_ratio == 0)
			return ReachabilityQuery_MT0(start_id, rect);
		//merge and multi-layer
		else
		{
			String query = String.format("match (n) where id(n) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", start_id,  GeoB_name, bitmap_name, RMBR_minx_name, RMBR_miny_name, RMBR_maxx_name,RMBR_maxy_name);
			String result = Neo4j_Graph_Store.Execute(resource, query);
			JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
			jsonArr = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
			
			//start_id vertex cannot reach any spatial vertices
			if(jsonArr.get(0).isJsonNull()&&jsonArr.get(1).isJsonNull()&&jsonArr.get(2).isJsonNull())
			{
				return false;
			}
			
			else
			{
				HashMap<Integer,Integer> lb_x_hash = new HashMap<Integer,Integer>();
				HashMap<Integer,Integer> lb_y_hash = new HashMap<Integer,Integer>();
				HashMap<Integer,Integer> rt_x_hash = new HashMap<Integer,Integer>();
				HashMap<Integer,Integer> rt_y_hash = new HashMap<Integer,Integer>();
				
				for(int level_pieces = 2;level_pieces<=128;level_pieces*=2)
			    {
					int lb_x = (int) ((rect.min_x - total_range.min_x)/multi_resolution_x.get(level_pieces));
					int lb_y = (int) ((rect.min_y - total_range.min_y)/multi_resolution_y.get(level_pieces));
					int rt_x = (int) ((rect.max_x - total_range.min_x)/multi_resolution_x.get(level_pieces));
					int rt_y = (int) ((rect.max_y - total_range.min_y)/multi_resolution_y.get(level_pieces));
					
					lb_x_hash.put(level_pieces, lb_x);
					lb_y_hash.put(level_pieces, lb_y);
					rt_x_hash.put(level_pieces, rt_x);
					rt_y_hash.put(level_pieces, rt_y);
					
			    }		
				
				//G-vertex
				if(!jsonArr.get(1).isJsonNull())
				{
					String ser = jsonArr.get(1).getAsString();
					ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(ser));
					ImmutableRoaringBitmap reachgrid = new ImmutableRoaringBitmap(newbb);
					
					int	outside_count = 0;
				    for(int level_pieces = split_pieces;level_pieces >=2;level_pieces/=2)
				    {
				    	int lb_x = lb_x_hash.get(level_pieces);
						int lb_y = lb_y_hash.get(level_pieces);
						int rt_x = rt_x_hash.get(level_pieces);
						int rt_y = rt_y_hash.get(level_pieces);
						
						int offset = multi_offset.get(level_pieces);
						
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
						if(flag == false)
						{
							outside_count++;
						}
						else
							break;
				    }
				    if(outside_count == level_count)
				    {
						return false;
				    }
				    else
				    	return TraverseQuery(start_id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
				}
				else
				{
					//R-vertex
					if(!jsonArr.get(2).isJsonNull())
					{
						MyRectangle RMBR = new MyRectangle(jsonArr.get(2).getAsDouble(), jsonArr.get(3).getAsDouble(), jsonArr.get(4).getAsDouble(), jsonArr.get(5).getAsDouble());
						
						//RMBR No overlap case
						if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
						{
							return false;
						}
						
						//RMBR Contain case
						if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_y && RMBR.max_y < rect.max_y)
						{
							return true;
						}
						
						return TraverseQuery(start_id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
					}
					else
					{
						return TraverseQuery(start_id, rect, lb_x_hash, lb_y_hash, rt_x_hash, rt_y_hash);
					}
				}
			}
		}
	}
}