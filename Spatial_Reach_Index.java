package def;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class Spatial_Reach_Index implements ReachabilityQuerySolver{

	private PostgresJDBC p_postgres = new PostgresJDBC();
	private String RTreeName;
	
	
	Spatial_Reach_Index(String p_RTreeName)
	{
		this.RTreeName = p_RTreeName;
	}
	
	private ResultSet RangeQuery(MyRectangle rect)
	{
		try
		{
			HashSet<Integer> hs = new HashSet<Integer>();
			Connection con = p_postgres.GetConnection();
			Statement st = con.createStatement();
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			ResultSet rs = st.executeQuery(query);
			return rs;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			return null;
		}
	}

	public void Preprocess() 
	{
		
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		try
		{
			Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
			String query = "match (n) where id(n)="+start_id+" return n";
			String result = p_neo.Execute(query);
			JsonArray jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			JsonObject jsonOb = jsonArr.get(0).getAsJsonObject();
			jsonArr = jsonOb.get("row").getAsJsonArray();
			jsonOb = jsonArr.get(0).getAsJsonObject();
			int id = jsonOb.get("id").getAsInt();
			
			query = "match (n:Reachability_Index) where n.id = " + id + " return n";
			result = p_neo.Execute(query);
			jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			jsonOb = jsonArr.get(0).getAsJsonObject();
			jsonArr = jsonOb.get("row").getAsJsonArray();
			jsonOb = jsonArr.get(0).getAsJsonObject();
			int source_scc_id = jsonOb.get("scc_id").getAsInt();
			
			Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
			ArrayList<Integer> source_reachTo = new Gson().fromJson(jsonOb.get("reachTo"), listType);

			ResultSet rs = this.RangeQuery(rect);
			int bulksize = 2000;
			int i = 0;
			while(rs.next())
			{
				int target_id = Integer.parseInt(rs.getObject("id").toString());
				query = "match (n:Reachability_Index) where n.id = " + target_id + " return n";
				result = p_neo.Execute(query);
				jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
				jsonOb = jsonArr.get(0).getAsJsonObject();
				jsonArr = jsonOb.get("row").getAsJsonArray();
				jsonOb = jsonArr.get(0).getAsJsonObject();
				int target_scc_id = jsonOb.get("scc_id").getAsInt();
				if(source_scc_id > target_scc_id)
					continue;
				else
				{
					ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
					int sn = source_reachTo.size(), tn = target_reachFrom.size();
				    int si = 0, ti = 0;
				    
				    while(si < sn && ti < tn) {
				        int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
				        if (sp == tp) {
				        	System.out.println(source_scc_id);
				        	System.out.println(target_scc_id);
				        	System.out.println(source_reachTo.get(si));
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
			return false;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			return false;
		}
		
	}
}
