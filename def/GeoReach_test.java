package def;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GeoReach_test 
{	
	private static GeoReach p_georeach = new GeoReach();
	
	public static void Reconstruct_test()
	{
		Config p_con = new Config();
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		GeoReach p_geo = new GeoReach();
		long graph_size = OwnMethods.GetNodeCount("Patents");
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, 500);
//		Iterator<String> iter = hs.iterator();
//		while(iter.hasNext())
		boolean flag = true;
		for(int j = 2;j<=4;j++)
		{
			for(int i = 0;i<graph_size;i++)
			{
//				long id = 3*graph_size+Long.parseLong(iter.next());
				long id = graph_size*j+i;
				System.out.println(id);
				MyRectangle update = p_geo.Reconstruct(id);
				String query = String.format("match (a) where id(a) = %d return a", id);
				String result = p_neo.Execute(query);
				JsonArray jarr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
				JsonObject jobj  = jarr.get(0).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsJsonObject();
				if(!jobj.has(p_con.GetRMBR_minx_name()))
				{
					if(update == null)
						OwnMethods.Println(true);
					else
					{
						OwnMethods.Println(false);
						flag = false;
						break;
					}
					
				}
				else
				{
					if(update == null)
					{
						OwnMethods.Println(false);
						flag = false;
						break;
					}
					else
					{
						if(Math.abs(jobj.get(p_con.GetRMBR_minx_name()).getAsDouble()-update.min_x)>0.00000001||Math.abs(jobj.get(p_con.GetRMBR_miny_name()).getAsDouble()-update.min_y)>0.00000001||Math.abs(jobj.get(p_con.GetRMBR_maxx_name()).getAsDouble()-update.max_x)>0.00000001||Math.abs(jobj.get(p_con.GetRMBR_maxy_name()).getAsDouble()-update.max_y)>0.00000001)
						{
							OwnMethods.Println(false);
							flag = false;
							break;
						}
						else
							OwnMethods.Println(true);
					}
				}
				if(flag == false)
					break;
			}
			if(flag == false)
				break;
		}	
	}
	
	public static void main(String[] args)
	{
		Reconstruct_test();
		/*Rectangle rect = p_georeach.GetRMBR(17585);
		System.out.println(rect.min_x);
		System.out.println(rect.min_y);
		System.out.println(rect.max_x);
		System.out.println(rect.max_y);*/
		
		/*long start = System.currentTimeMillis();
		p_georeach.Preprocess();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		
//		double x = 356.06470243024137, y = 868.148461610409;
//		
//		MyRectangle rect = new MyRectangle(x,y,x+100,y+100);
//		System.out.println(p_georeach.ReachabilityQuery(9434538, rect));
		/*long sumtime = 0;
		for(int i = 0;i<50;i++)
		{
			long start = System.currentTimeMillis();
			System.out.println(p_georeach.ReachabilityQuery(12344377, rect));
			sumtime += System.currentTimeMillis() - start;
		}
		System.out.println(sumtime);*/
		
	}
}
