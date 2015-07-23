package def;

import java.util.*;

public class Experiment {

	public static void main(String[] args) {
		
		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		
		GeoReach georeach = new GeoReach();
		Index index = new Index();
		Traversal traversal = new Traversal();
		
		Rectangle query_rect = new Rectangle(0, 0, 500, 500);

		OwnMethods p_ownmethods = new OwnMethods();
		String root = "/home/yuhansun/Documents/Synthetic_data";
		
		//String filename = root + "/16000_1/test_graph_ids.txt";
		//String filename = root + "/65536_16_1/test_graph_ids.txt";
		//String filename = root + "/262144_18_1/test_graph_ids.txt";
		String filename = root + "/DAG/18_4/test_graph_ids.txt";
		
		
		ArrayList<String> graph_ids = p_ownmethods.ReadFile(filename);
		
		
		long time1 = 0,time2 = 0,time3 = 0;
		
		for(int i = 0;i<500;i++)
		{
			System.out.println(i);
			int id = Integer.parseInt(graph_ids.get(i));
			System.out.println(id);

			traversal.VisitedVertices.clear();
			long start = System.currentTimeMillis();
			boolean result1 = traversal.ReachabilityQuery(id, query_rect);
			time1+=System.currentTimeMillis() - start;
			System.out.println(result1);
			
			start = System.currentTimeMillis();
			//boolean result2 = index.ReachabilityQuery(id, query_rect,"RTree_262144_18_1","Transitive_closure_262144_18_1");
			//boolean result2 = index.ReachabilityQuery(id, query_rect);
			//boolean result2 = index.ReachabilityQuery(id, query_rect,"DAGRTree_18_1","DAGTransitive_closure_18_1");
			
			time2+=System.currentTimeMillis() - start;
			//System.out.println(result2);
			
			georeach.VisitedVertices.clear();
			start = System.currentTimeMillis();
			boolean result3 = georeach.ReachabilityQuery(id, query_rect);
			time3+=System.currentTimeMillis() - start;
			System.out.println(result3);
						
			//if(result1!=result2 || result1!=result3)
			if(result1!=result3)
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
