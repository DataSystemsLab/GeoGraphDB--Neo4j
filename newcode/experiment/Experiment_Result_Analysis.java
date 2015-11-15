//package experiment;
//import def.*;
//
//import java.util.ArrayList;
//
//public class Experiment_Result_Analysis {
//
//	public static void main(String[] args) {
//		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
//		
//		GeoReach georeach = new GeoReach();
//		Index index = new Index();
//		Traversal traversal = new Traversal();
//		
//		MyRectangle query_rect = new MyRectangle(0, 0, 600, 600);
//
//		OwnMethods p_ownmethods = new OwnMethods();
//		String root = "/home/yuhansun/Documents/Synthetic_data";
//		
//		//String filename = root + "/16000_1/test_graph_ids.txt";
//		//String filename = root + "/65536_16_1/test_graph_ids.txt";
//		//String filename = root + "/262144_18_1/test_graph_ids.txt";
//		String filename = root + "/18_16/test_graph_ids.txt";
//		//String filename = root + "/DAG/18_4/test_graph_ids.txt";
//		
//		ArrayList<String> graph_ids = p_ownmethods.ReadFile(filename);
//		
//		int true_count = 0,false_count = 0;
//				
//		for(int i = 0;i<500;i++)
//		{
//			System.out.println(i);
//			int id = Integer.parseInt(graph_ids.get(i));
//			System.out.println(id);
//
//			traversal.VisitedVertices.clear();
//			boolean result1 = traversal.ReachabilityQuery(id, query_rect);
//			System.out.println(result1);
//			
//			//boolean result2 = index.ReachabilityQuery(id, query_rect,"RTree_262144_18_4","Transitive_closure_262144_18_4");
//			//boolean result2 = index.ReachabilityQuery(id, query_rect);
//			//boolean result2 = index.ReachabilityQuerySCC(id, query_rect,"RTree_18_4","SCCTransitive_closure_18_4");
//			//boolean result2 = index.ReachabilityQuery(id, query_rect,"DAGRTree_18_4","DAGTransitive_closure_18_4");
//			
//			//System.out.println(result2);
//			
//			
////			georeach.VisitedVertices.clear();
////			boolean result3 = georeach.ReachabilityQuery(id, query_rect);
////			System.out.println(result3);
//			
//			if(result1)
//				true_count+=1;
//			else
//				false_count+=1;
//						
//		}
//		System.out.println(true_count);
//		System.out.println(false_count);
//	}
//
//}
