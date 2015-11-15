//package experiment;
//import def.*;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Random;
//public class Experiment_8_27 
//{
//	private static long graph_size;
//	private static double experiment_node_count = 500.0;
//	private static double spatial_total_range = 1000;
//	
//	public static void main(String[] args) 
//	{
//		String datasource = "Patents";
//		String result_file_path = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_27.csv";
//		graph_size = OwnMethods.GetNodeCount(datasource);
//		int ratio = 80;
//		OwnMethods.WriteFile(result_file_path, true, "spatial_range\t"+"Spa\tRangeQuery\tTraversal\tInMemoryJudge\tAccessNodeCount\tNeighborOperationCount\tInRangeCount\tSpa-Reach\tRangeQuery\tReachability\tInMemoryJudge\tInRangeCount\tAccessNodeCount\tNeo4jAccessCount\tGeoReach\tTraversal\tInMemoryJudge\tAccessNodeCount\tNeighborOperationCount\ttrue_result_count\tFalseNoExistCount\tFalseNotReachCount\n");
//		String graph_label = "Graph_Random_" + ratio;
//
//		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		
//		ArrayList<Double> a_x = new ArrayList<Double>();
//		ArrayList<Double> a_y = new ArrayList<Double>();
//		
//		Random r = new Random();
//		for(double j = 0.001;j<101;j*=10)
//		{
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);	
//			
//			a_x.clear();
//			a_y.clear();
//			for(int i = 0;i<experiment_node_count;i++)
//			{
//				a_x.add(r.nextDouble()*(1000-rect_size));
//				a_y.add(r.nextDouble()*(1000-rect_size));
//			}
//			
//			long time_spa = 0, time_spareach = 0, time_georeach = 0;
//			int false_no_exist = 0, false_not_reach = 0, true_count = 0;
////			//spatial index
//			System.out.println(OwnMethods.ClearCache());
//			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//			System.out.println(PostgresJDBC.StartServer());
//			SpatialIndex spa = new SpatialIndex(datasource + "_Random_" + ratio);
//			int accessnodecount = 0, inrangecount = 0;
////			for(int i = 0;i<al.size();i++)
////			{
////				double x = a_x.get(i);
////				double y = a_y.get(i);
////				MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
////				
////				System.out.println(i);
////				int id = Integer.parseInt(al.get(i));
////				System.out.println(id);
////								
////				try 
////				{
////					long start = System.currentTimeMillis();
////					boolean result4 = spa.ReachabilityQuery(id, query_rect);
////					time_spa += System.currentTimeMillis() - start;
////					System.out.println(result4);
////					accessnodecount+=spa.VisitedVertices.size();
////					inrangecount+=spa.InRangeCount;
////					if(result4)
////						true_count+=1;
////					else
////					{
////						if(spa.InRangeCount == 0)
////							false_no_exist+=1;
////						else
////							false_not_reach+=1;
////					}
////				} 
////				catch(Exception e)
////				{
////					e.printStackTrace();
////					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
////					i = i-1;
////				}
////			}
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_27.csv", true, j/100.0+"\t"+time_spa/experiment_node_count+"\t"+spa.PostgresTime/experiment_node_count+"\t"+spa.Neo4jTime/experiment_node_count+"\t"+spa.JudgeTime/experiment_node_count+"\t"+accessnodecount/experiment_node_count+"\t"+spa.NeighborOperationCount/experiment_node_count+"\t"+inrangecount/experiment_node_count+"\t");
//			spa.Disconnect();
//			
//			//spatial-reachability index
//			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//			System.out.println(PostgresJDBC.StopServer());
//			System.out.println(OwnMethods.ClearCache());
//			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//			System.out.println(PostgresJDBC.StartServer());
//			Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
//			int inrangetotalcount = 0;
//			for(int i = 0;i<al.size();i++)
//			{
//				double x = a_x.get(i);
//				double y = a_y.get(i);
//				MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//				
//				System.out.println(i);
//				int id = Integer.parseInt(al.get(i));
//				System.out.println(id);
//				
//				try 
//				{
//					long start = System.currentTimeMillis();
//					boolean result2 = spareach.ReachabilityQuery(id, query_rect);
//					time_spareach += (System.currentTimeMillis() - start);
//					System.out.println(result2);
//					int count = spareach.GetInRangeCount();
//					inrangetotalcount+=count;					
//					if(result2)
//						true_count+=1;
//					else
//					{
//						if(count == 0)
//							false_no_exist+=1;
//						else
//							false_not_reach+=1;
//					}
//					spareach.CloseResultSet();
//				} 
//				catch(Exception e)
//				{
//					e.printStackTrace();
//					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//					i = i-1;
//				}				
//			}
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_27.csv", true, time_spareach/experiment_node_count+"\t"+spareach.postgresql_time/experiment_node_count+"\t"+spareach.neo4j_time/experiment_node_count+"\t"+spareach.judge_time/experiment_node_count+"\t"+inrangetotalcount/experiment_node_count+"\t"+spareach.AccessNodeCount/experiment_node_count+"\t"+spareach.Neo4jAccessCount/experiment_node_count+"\t");
//			spareach.Disconnect();
//			PostgresJDBC.StopServer();
//			
//			//georeach
////			System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
//			MyRectangle rect = new MyRectangle(0,0,1000,1000); 
//			GeoReach_Integrate georeach = new GeoReach_Integrate(rect, 5);
//			accessnodecount = 0;
////			for(int i = 0;i<al.size();i++)
////			{
////				double x = a_x.get(i);
////				double y = a_y.get(i);
////				MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
////				
////				System.out.println(i);
////				int id = Integer.parseInt(al.get(i));
////				System.out.println(id);
////				
////				try
////				{
////					georeach.VisitedVertices.clear();
////					long start = System.currentTimeMillis();
////					boolean result3 = georeach.ReachabilityQuery(id, query_rect);
////					time_georeach += System.currentTimeMillis() - start;
////					System.out.println(result3);
////					accessnodecount+=georeach.VisitedVertices.size();
////				}
////				catch(Exception e)
////				{
////					e.printStackTrace();
////					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
////					i = i-1;
////				}
////										
////			}
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_27.csv", true, time_georeach/experiment_node_count+"\t"+georeach.neo4j_time/experiment_node_count+"\t"+georeach.judge_time/experiment_node_count+"\t"+accessnodecount/experiment_node_count+"\t"+georeach.Neo4jAccessCount/experiment_node_count+"\t");
//			Neo4j_Graph_Store.StopMyServer(datasource);
//			
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_27.csv", true, true_count+"\t"+false_no_exist+"\t"+false_not_reach+"\n");
//		}
//	}
//
//}
