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
	
	private WebResource resource;
	private Connection con;
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet<Integer>();
	
	public SpatialIndex(String p_RTreeName)
	{
		p_neo = new Neo4j_Graph_Store();
		resource = p_neo.GetCypherResource();
		p_config = new Config();
		RTreeName = p_RTreeName;
		con = PostgresJDBC.GetConnection();
		
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
		
	public static void CreateTable(String datasource)
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
					String query = "create table "+datasource+"_Random_" + ratio + " (id bigint,";
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

	public static void LoadData(String datasource)
	{
		File file = null;
		BufferedReader reader = null;
		Connection con = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			con.setAutoCommit(false);
			//insert data
			for(int ratio = 20;ratio<100;ratio+=20)
			{
				System.out.println("load "+datasource+"_Random_" + ratio);
				String filename = "/home/yuhansun/Documents/Real_data/"+datasource+"/Random_spatial_distributed/" + ratio + "/entity.txt";
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
					String tablename = datasource + "_Random_" + ratio;
					String query = "insert into " + tablename + " values (" + l[0] + ", '" + l[2] + "," + l[3] + "')";
					Statement st = con.createStatement();
					st.executeUpdate(query);
					st.close();
				}
				reader.close();
				con.commit();
			}
			con.setAutoCommit(true);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			PostgresJDBC.Close(con);
		}
	}

	public static void CreateGistIndex(String datasource)
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
				String tablename = datasource + "_Random_" + ratio;
				String query = "CREATE INDEX "+datasource+"_Random_"+ratio+"_Gist ON "+tablename+" USING gist(location)";
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
		CreateTable(datasource);
		LoadData(datasource);
		CreateGistIndex(datasource);

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
			Statement st = con.createStatement();
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			ResultSet rs = st.executeQuery(query);
			while(rs.next())
			{
				long id = Long.parseLong(rs.getObject("id").toString());
				hs.add((int)id);
			}
			PostgresTime += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			
			query = "match (a)-->(b) where id(a) = " +Integer.toString(start_id) +" return id(b),b";
			
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
				start = System.currentTimeMillis();
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
			
			while(!queue.isEmpty())
			{
				start = System.currentTimeMillis();
				
				int id = queue.poll();
				
				query = "match (a)-->(b) where id(a) = " +Integer.toString(id) +" return id(b), b";
				
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
	}
	
	

}
