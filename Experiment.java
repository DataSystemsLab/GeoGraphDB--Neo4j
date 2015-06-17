package def;

import java.util.*;

public class Experiment {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		
		GeoReach georeach = new GeoReach();
		Index index = new Index();
		Traversal traversal = new Traversal();
		
		Rectangle query_rect = new Rectangle(-130, 20, -60, 50);
		
		String query = "match (a) where has(a.RMBR_minx) return id(a) limit 10";
		String result = p_neo4j_graph_store.Execute(query);
		System.out.println(result);
		ArrayList<String> ids = p_neo4j_graph_store.GetExecuteResultData(result);
		
		long time1 = 0,time2 = 0,time3 = 0;
		
		for(int i = 0;i<ids.size();i++)
		{
			System.out.println(i);
			int id = Integer.parseInt(ids.get(i));

			long start = System.currentTimeMillis();
			boolean result1 = traversal.ReachabilityQuery(id, query_rect);
			time1+=System.currentTimeMillis() - start;
			
			start = System.currentTimeMillis();
			boolean result2 = index.ReachabilityQuery(id, query_rect);
			time2+=System.currentTimeMillis() - start;
			
			start = System.currentTimeMillis();
			boolean result3 = georeach.ReachabilityQuery(id, query_rect);
			time3+=System.currentTimeMillis() - start;
			
			System.out.printf("%s\n%s\n%s\n\n", result1, result2, result3);
			
			if(result1!=result2 || result1!=result3)
			{
				System.out.println(id);
				break;
			}
		}
		
		System.out.printf("%s, %s, %s\n", time1, time2, time3);
		System.out.printf("%s, %s, %s\n", index.GetTranTime, index.GetRTreeTime, index.JudgeTime);
		System.out.printf("%s, %s",index.QueryTime, index.BuildListTime);
	}

}
