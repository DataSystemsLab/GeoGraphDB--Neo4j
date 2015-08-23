package def;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.WebResource;

public class Spatial_Reach_Index implements ReachabilityQuerySolver{

	private String RTreeName;
	private Connection con;
	private Statement st;
	private ResultSet rs;
	private WebResource resource;
	
	public long postgresql_time;
	public long neo4j_time;
	public long judge_time;
	
	public Spatial_Reach_Index(String p_RTreeName)
	{
		this.RTreeName = p_RTreeName;
		con = PostgresJDBC.GetConnection();
		try {
			st = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		resource = p_neo.GetCypherResource();
		
		postgresql_time = 0;
		neo4j_time = 0;
		judge_time = 0;
	}
	
	public void Disconnect()
	{
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
	}
	
	private void RangeQuery(MyRectangle rect)
	{
		try
		{			
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			rs = st.executeQuery(query);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void Preprocess() 
	{
		
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		try
		{
			String query = "match (n) where id(n)="+start_id+" return n";
			
			long start = System.currentTimeMillis();
			String result = Neo4j_Graph_Store.Execute(resource, query);
			JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
			neo4j_time+=System.currentTimeMillis() - start;
			
			JsonObject jsonOb = jsonArr.get(0).getAsJsonObject();
			jsonArr = jsonOb.get("row").getAsJsonArray();
			jsonOb = jsonArr.get(0).getAsJsonObject();
			int id = jsonOb.get("id").getAsInt();
			
			query = "match (n:Reachability_Index) where n.id = " + id + " return n";
			
			start = System.currentTimeMillis();
			result = Neo4j_Graph_Store.Execute(resource, query);
			jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
			neo4j_time += System.currentTimeMillis() - start;
			
			jsonOb = jsonArr.get(0).getAsJsonObject();
			jsonArr = jsonOb.get("row").getAsJsonArray();
			jsonOb = jsonArr.get(0).getAsJsonObject();
			int source_scc_id = jsonOb.get("scc_id").getAsInt();
			
			Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
			ArrayList<Integer> source_reachTo = new Gson().fromJson(jsonOb.get("reachTo"), listType);

			start = System.currentTimeMillis();
			this.RangeQuery(rect);
			postgresql_time+=System.currentTimeMillis() - start;
			int bulksize = 500;
			int i = 0;
			while(rs.next())
			{				
				if(i == 0)
				{
					query = "match (n:Reachability_Index) where n.id in ["+rs.getString("id").toString();
					i++;
					continue;
				}

				
				if(i == bulksize-1)
				{
					query += (","+rs.getString("id").toString()+"] return n");
					i = 0;
					
					start = System.currentTimeMillis();
					result = Neo4j_Graph_Store.Execute(resource, query);
					jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
					neo4j_time+=System.currentTimeMillis() - start;
					
					start = System.currentTimeMillis();
					for(int j = 0;j<jsonArr.size();j++)
					{
						jsonOb = jsonArr.get(j).getAsJsonObject();
						JsonArray row = jsonOb.get("row").getAsJsonArray();
						jsonOb = row.get(0).getAsJsonObject();
						int target_scc_id = jsonOb.get("scc_id").getAsInt();
						if(source_scc_id >= target_scc_id)
							continue;
						else
						{
							ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
							int sn = source_reachTo.size(), tn = target_reachFrom.size();
						    int si = 0, ti = 0;
						    
						    while(si < sn && ti < tn) 
						    {
						        int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
						        if (sp == tp) {
//						        	System.out.println(source_scc_id);
//						        	System.out.println(target_scc_id);
//						        	System.out.println(source_reachTo.get(si));
						        	judge_time += System.currentTimeMillis() - start;
						            return true;
						        }
						        if (sp <= tp) {
						            si++;
						        } else {
						            ti++;
						        }
						    }
						    continue;
						}
					}
					judge_time += System.currentTimeMillis() - start;
				}
				else
				{
					query+= ("," + rs.getString("id").toString());
					i++;
				}
			}
			
			if(i!=0)
			{
				query+="] return n";
				
				start = System.currentTimeMillis();
				result = Neo4j_Graph_Store.Execute(resource, query);
				jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
				neo4j_time += System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				for(int j = 0;j<jsonArr.size();j++)
				{
					jsonOb = jsonArr.get(j).getAsJsonObject();
					JsonArray row = jsonOb.get("row").getAsJsonArray();
					jsonOb = row.get(0).getAsJsonObject();
					int target_scc_id = jsonOb.get("scc_id").getAsInt();
					if(source_scc_id >= target_scc_id)
						continue;
					else
					{
						ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
						int sn = source_reachTo.size(), tn = target_reachFrom.size();
					    int si = 0, ti = 0;
					    
					    while(si < sn && ti < tn) 
					    {
					        int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
					        if (sp == tp) {
//					        	System.out.println(source_scc_id);
//					        	System.out.println(target_scc_id);
//					        	System.out.println(source_reachTo.get(si));
					        	judge_time += System.currentTimeMillis();
					            return true;
					        }
					        if (sp <= tp) {
					            si++;
					        } else {
					            ti++;
					        }
					    }
					    continue;
					}
				}
				judge_time += System.currentTimeMillis() - start;
			}
//			while(rs.next())
//			{
//				int target_id = Integer.parseInt(rs.getObject("id").toString());
//				query = "match (n:Reachability_Index) where n.id = " + target_id + " return n";
//				result = p_neo.Execute(query);
//				jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
//				jsonOb = jsonArr.get(0).getAsJsonObject();
//				jsonArr = jsonOb.get("row").getAsJsonArray();
//				jsonOb = jsonArr.get(0).getAsJsonObject();
//				int target_scc_id = jsonOb.get("scc_id").getAsInt();
//				if(source_scc_id > target_scc_id)
//					continue;
//				else
//				{
//					ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
//					int sn = source_reachTo.size(), tn = target_reachFrom.size();
//				    int si = 0, ti = 0;
//				    
//				    while(si < sn && ti < tn) {
//				        int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
//				        if (sp == tp) {
//				        	System.out.println(source_scc_id);
//				        	System.out.println(target_scc_id);
//				        	System.out.println(source_reachTo.get(si));
//				            return true;
//				        }
//				        if (sp <= tp) {
//				            si++;
//				        } else {
//				            ti++;
//				        }
//				    }
//				    continue;
//				}
//			}						
			return false;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		finally
		{
			PostgresJDBC.Close(rs);
		}
		
	}
}
