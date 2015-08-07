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

public class SpatialIndex implements ReachabilityQuerySolver{
	
	private Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
	private Index p_index = new Index();
	private OwnMethods p_own = new OwnMethods();
	private PostgresJDBC p_postgres = new PostgresJDBC();
	private String longitude_property_name;
	private String latitude_property_name;
	Config p_config;
	private String RTreeName;
	
	public long PostgresTime;
	public long Neo4jTime;
	public long JudgeTime;
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	public SpatialIndex(String p_RTreeName)
	{
		p_neo = new Neo4j_Graph_Store();
		p_index = new Index();
		p_own = new OwnMethods();
		p_postgres = new PostgresJDBC();
		p_config = new Config();
		longitude_property_name = p_config.GetLongitudePropertyName();
		latitude_property_name = p_config.GetLatitudePropertyName();
		RTreeName = p_RTreeName;
		
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

	public void Construct_RTree_Index()
	{
		File file = null;
		BufferedReader reader = null;
		try
		{
			Connection con = p_postgres.GetConnection();
			
			//create table
//			for(int ratio = 20;ratio<60;ratio+=20)
//			{
//				Statement st = con.createStatement();
//				String query = "create table Patents_Random_" + ratio + " (id bigint,";
//				query+=("location point)");
//				System.out.println(query);
//				st.executeUpdate(query);
//				
//			}
			
			//insert data
//			for(int ratio = 20;ratio<100;ratio+=20)
//			{
//				String filename = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/" + ratio + "/spatial_entity.txt";
//				file = new File(filename);
//				reader = new BufferedReader(new FileReader(file));
//				reader.readLine();
//				String tempString = null;
//				while((tempString = reader.readLine())!=null)
//				{
//					String[] l = tempString.split(" ");
//					String tablename = "Patents_Random_" + ratio;
//					String query = "insert into " + tablename + " values (" + l[0] + ", '" + l[2] + "," + l[3] + "')";
//					System.out.println(query);
//					Statement st = con.createStatement();
//					st.executeUpdate(query);
//					st.close();
//				}
//				reader.close();
//			}
			
			//create gist index
//			for(int ratio = 20;ratio<100;ratio+=20)
//			{
//				String tablename = "Patents_Random_" + ratio;
//				String query = "CREATE INDEX Patents_Random_"+ratio+"_Gist ON "+tablename+" USING gist(location)";
//				Statement st = con.createStatement();
//				st.executeUpdate(query);
//				st.close();
//			}
			
			con.close();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}

	}
	
	public void Preprocess() 
	{
		
		Construct_RTree_Index();
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		try
		{
			Queue<Integer> queue = new LinkedList();
			VisitedVertices.clear();
			
			long start = System.currentTimeMillis();
			
			HashSet<Integer> hs = new HashSet<Integer>();
			Connection con = p_postgres.GetConnection();
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
			
			String result = p_neo.Execute(query);
			
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
				
				result = p_neo.Execute(query);
				
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