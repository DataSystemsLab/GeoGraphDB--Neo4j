package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql. * ;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.WebResource;

public class SpatialIndex implements ReachabilityQuerySolver{
	
	private Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
	Config p_config;
	private String RTreeName;
	
	public long PostgresTime;
	public long Neo4jTime;
	public long JudgeTime;
	
	public int NeighborOperationCount = 0;
	public int InRangeCount = 0;
	
	private WebResource resource;
	private Connection con;
	private Statement st;
	private ResultSet rs;
	
	//used in query procedure in order to record visited vertices
	public Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	public SpatialIndex(String p_RTreeName)
	{
		p_neo = new Neo4j_Graph_Store();
		resource = p_neo.GetCypherResource();
		p_config = new Config();
		RTreeName = p_RTreeName;
		con = PostgresJDBC.GetConnection();
		try {
			st = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Neo4jTime = 0;
		PostgresTime = 0;
		JudgeTime = 0;
	}
	
	
//	public void Construct_RTree_Index()
//	{
//		for(int ratio = 20;ratio<100;ratio+=20)
//		{
//			long database_size = p_own.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db"));
//			
//			String label = "RTree_Random_" + ratio;
//			String tree_name = "RTree_Random_" + ratio;
//			
//			p_index.CreatePointLayer(tree_name);
//			
//			String query = "match (a:"+label+") return id(a)";
//			String result = p_neo.Execute(query);
//			JsonArray jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
//			for(int j = 0;j<jsonArr.size();j++)
//			{
//				JsonObject jsonOb = (JsonObject) jsonArr.get(j);
//				JsonArray arr = jsonOb.get("row").getAsJsonArray();
//				long id = arr.get(0).getAsLong();
//				p_index.AddOneNodeToPointLayer(tree_name, id);
//			}
//			long rtree_size = p_own.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db")) - database_size;
//			System.out.println(tree_name + ": " + rtree_size);
//		}
//	}
	
	public static void DropTable(String datasource)
	{
		Connection con = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			for(int ratio = 20;ratio<100;ratio+=20)
			{
				Statement st = null;
				try
				{
					st = con.createStatement();
					String query = "drop table "+ datasource + "_Random_"+ratio;
					st.executeUpdate(query);
				}
				catch(Exception e)
				{
					System.out.println(e.getMessage());
				}
				finally
				{
					PostgresJDBC.Close(st);
				}	
			}
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			PostgresJDBC.Close(con);
		}		
	}
		
	public static void CreateTable(String datasource, String suffix)
	{
		Connection con = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			for(int ratio = 20;ratio<100;ratio+=20)
			{
				Statement st = null;
				try
				{
					st = con.createStatement();
					String query = "create table "+datasource+"_Random_" + ratio + suffix + " (id bigint,";
					query+=("location point)");
					System.out.println(query);
					st.executeUpdate(query);
				}
				catch(Exception e)
				{
					System.out.println(e.getMessage());
				}
				finally
				{
					PostgresJDBC.Close(st);
				}				
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			PostgresJDBC.Close(con);
		}	
	}
	
	public static void DropTable(String datasource, String suffix)
	{
		Connection con = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			for(int ratio = 20;ratio<100;ratio+=20)
			{
				Statement st = null;
				try
				{
					st = con.createStatement();
					String query = "drop table "+datasource+"_Random_" + ratio + suffix;
					System.out.println(query);
					st.executeUpdate(query);
				}
				catch(Exception e)
				{
					System.out.println(e.getMessage());
				}
				finally
				{
					PostgresJDBC.Close(st);
				}				
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			PostgresJDBC.Close(con);
		}	
	}
	
//	public static void LoadDataPrepare(String datasource)
//	{
//		File file = null;
//		BufferedReader reader = null;
//		Connection con = null;
//		PreparedStatement pst = null;
//		try
//		{
//			con = PostgresJDBC.GetConnection();
//			con.setAutoCommit(false);
//			//insert data
////			for(int ratio = 20;ratio<100;ratio+=20)
//			int ratio = 80;
//			{
//				String tablename = datasource + "_Random_" + ratio;
//				String query = "insert into " + tablename + "(id, location) values (?,?)";
//				pst = con.prepareStatement(query);
//				
//				System.out.println("load "+datasource+"_Random_" + ratio);
//				String filename = "/home/yuhansun/Documents/Real_data/"+datasource+"/Random_spatial_distributed/" + ratio + "/entity.txt";
//				file = new File(filename);
//				reader = new BufferedReader(new FileReader(file));
//				reader.readLine();
//				String tempString = null;
//				while((tempString = reader.readLine())!=null)
//				{
//					String[] l = tempString.split(" ");
//					int isspatial = Integer.parseInt(l[1]);
//					if(isspatial == 0)
//						continue;
//					
//					pst.setLong(1, Integer.parseInt(l[0]));
//					pst.setObject(2, x);
//					st = con.createStatement();
//					st.executeUpdate(query);
//					st.close();
//				}
//				reader.close();
//				con.commit();
//			}
//			con.setAutoCommit(true);
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//			PostgresJDBC.Close(st);
//			PostgresJDBC.Close(con);
//		}
//	}

	public static void LoadData(String datasource, String suffix, String filesuffix)
	{
		File file = null;
		BufferedReader reader = null;
		Connection con = null;
		Statement st = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			con.setAutoCommit(false);
			st = con.createStatement();
			//insert data
			for(int ratio = 20;ratio<100;ratio+=20)
			
			//int ratio = 20;
			{
				System.out.println("load "+datasource+"_Random_" + ratio + suffix);
				String filename = "/home/yuhansun/Documents/Real_data/"+datasource+"/"+filesuffix+"/" + ratio + "/entity.txt";
				file = new File(filename);
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial == 0)
						continue;
					String tablename = datasource + "_Random_" + ratio + suffix;
					String query = "insert into " + tablename + " values (" + l[0] + ", '" + l[2] + "," + l[3] + "')";
					st.executeUpdate(query);
				}
				reader.close();
				con.commit();
			}
			st.close();
			con.setAutoCommit(true);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			PostgresJDBC.Close(st);
			PostgresJDBC.Close(con);
		}
	}

	public static void CreateGistIndex(String datasource, String suffix)
	{
		Connection con = null;
		try
		{			
			con = PostgresJDBC.GetConnection();
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/gist_index_time.txt", true,"ratio\tconstruct_time\n");
			//create gist index
			for(int ratio = 20;ratio<100;ratio+=20)
			{
				long start = System.currentTimeMillis();
				String tablename = datasource + "_Random_" + ratio+suffix;
				String query = "CREATE INDEX "+datasource+"_Random_"+ratio+suffix+"_Gist ON "+tablename+" USING gist(location)";
				System.out.println(query);
				Statement st = con.createStatement();
				st.executeUpdate(query);
				st.close();
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/gist_index_time.txt", true, ""+ratio+"\t"+(System.currentTimeMillis()-start)+"\n");
			}			
			con.close();
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			PostgresJDBC.Close(con);
		}

	}
	
	public static void Construct_RTree_Index(String datasource)
	{
//		CreateTable(datasource);
//		LoadData(datasource);
//		CreateGistIndex(datasource);

	}
	
	public void Preprocess() 
	{		
		//Construct_RTree_Index();
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		try
		{
			Queue<Integer> queue = new LinkedList<Integer>();
			VisitedVertices.clear();
			
			long start = System.currentTimeMillis();
			
			HashSet<Integer> hs = new HashSet<Integer>();
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			rs = st.executeQuery(query);
			while(rs.next())
			{
				long id = Long.parseLong(rs.getObject("id").toString());
				hs.add((int)id);
			}
			PostgresTime += System.currentTimeMillis() - start;
			
			InRangeCount = hs.size();
			
			if(hs.size() == 0)
			{
				
				return false;
			}
						
			start = System.currentTimeMillis();
			
			query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b),b";
			NeighborOperationCount+=1;
			
			String result = Neo4j_Graph_Store.Execute(resource, query);
			
			JsonParser jsonParser = new JsonParser();
			JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
			
			JsonArray jsonArr = (JsonArray) jsonObject.get("results");
			jsonObject = (JsonObject) jsonArr.get(0);
			jsonArr = (JsonArray) jsonObject.get("data");

			Neo4jTime += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			
			for(int i = 0;i<jsonArr.size();i++)
			{	
				jsonObject = (JsonObject)jsonArr.get(i);
				JsonArray row = (JsonArray)jsonObject.get("row");
				
				int id = row.get(0).getAsInt();
				
				jsonObject = (JsonObject)row.get(1);
				if(jsonObject.has("longitude"))
				{
					int attribute_id = jsonObject.get("id").getAsInt();
					if(hs.contains(attribute_id))
					{
						JudgeTime += System.currentTimeMillis() - start;
						System.out.println(id);
						return true;
					}
				}
				if(!VisitedVertices.contains(id))
				{
					VisitedVertices.add(id);
					queue.add(id);
				}
			}
			
			JudgeTime += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			
			while(!queue.isEmpty())
			{
				start = System.currentTimeMillis();
				
				int id = queue.poll();
				
				query = "match (a)-->(b) where id(a) = " +Integer.toString(id) +" return id(b), b";
				NeighborOperationCount+=1;
				
				result = Neo4j_Graph_Store.Execute(resource, query);
				
				jsonParser = new JsonParser();
				jsonObject = (JsonObject) jsonParser.parse(result);
				
				jsonArr = (JsonArray) jsonObject.get("results");
				jsonObject = (JsonObject) jsonArr.get(0);
				jsonArr = (JsonArray) jsonObject.get("data");
				
				Neo4jTime += System.currentTimeMillis() - start;
				start = System.currentTimeMillis();
				
				for(int i = 0;i<jsonArr.size();i++)
				{			
					jsonObject = (JsonObject)jsonArr.get(i);
					JsonArray row = (JsonArray)jsonObject.get("row");
					
					int neighbor_id = row.get(0).getAsInt();
					
					jsonObject = (JsonObject)row.get(1);
					if(jsonObject.has("longitude"))
					{
						int attribute_id = jsonObject.get("id").getAsInt();
						if(hs.contains(attribute_id))
						{
							JudgeTime += System.currentTimeMillis() - start;
							System.out.println(id);
							return true;
						}
					}				
					if(!VisitedVertices.contains(neighbor_id))
					{
						VisitedVertices.add(neighbor_id);
						queue.add(neighbor_id);
					}
				}
			}
			JudgeTime += System.currentTimeMillis() - start;
			
			return false;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			return false;
		}
		finally
		{
			PostgresJDBC.Close(rs);
		}
	}
	
	public void Disconnect()
	{
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
	}

}
