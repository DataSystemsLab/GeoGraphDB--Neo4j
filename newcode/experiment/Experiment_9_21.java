//package experiment;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Random;
//
//import def.GeoReach_Integrate;
//import def.MyRectangle;
//import def.Neo4j_Graph_Store;
//import def.OwnMethods;
//import def.PostgresJDBC;
//import def.Spatial_Reach_Index;
//
//public class Experiment_9_21 {
//
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		String datasource = "Patents";
//		String resultpath = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_9_21.csv";
//		OwnMethods.WriteFile(resultpath, true, "spatial_range\t");
////		for(int pieces = 128;pieces>=64;pieces/=2)
//		int pieces = 128;
//		OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_9_21.csv", true, "GeoReach_old\tGeoReach_new\tSpaReach\t");
//		OwnMethods.WriteFile(resultpath, true, "true_count\n");
//
//		int ratio = 80;
//		String graph_label = "Graph_Random_" + ratio;
//		long graph_size = OwnMethods.GetNodeCount(datasource);
//		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//		long experiment_node_count = 500;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		
//		ArrayList<Double> a_x = new ArrayList<Double>();
//		ArrayList<Double> a_y = new ArrayList<Double>();
//		
//		Random r = new Random();
////		a_x.clear();
////		a_y.clear();
////		for(int i = 0;i<experiment_node_count;i++)
////		{
////			a_x.add(r.nextDouble()*(1000));
////			a_y.add(r.nextDouble()*(1000));
////		}
//		
//		double selectivity = 0.0001;
//		double spatial_total_range = 1000;
//		boolean isbreak = false;
//		{
//			while(selectivity<=1)
//			{
//				double rect_size = spatial_total_range * Math.sqrt(selectivity);
//				OwnMethods.WriteFile(resultpath, true, selectivity+"\t");
//				
//				a_x.clear();
//				a_y.clear();
//				for(int i = 0;i<experiment_node_count;i++)
//				{
//					a_x.add(r.nextDouble()*(1000-rect_size));
//					a_y.add(r.nextDouble()*(1000-rect_size));
//				}
//				
//				int true_count = 0;
//				ArrayList<Boolean> geoold_result = new ArrayList<Boolean>();
//				ArrayList<Boolean> geonew_result = new ArrayList<Boolean>();
//				ArrayList<Boolean> spareach_result = new ArrayList<Boolean>();
////				for(int pieces = 128;pieces>=64;pieces/=2)
//				{
//					//GeoReach_Old
//					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
//					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//					MyRectangle rect = new MyRectangle(0,0,1000,1000); 
//					GeoReach_Integrate georeach = new GeoReach_Integrate(rect, pieces);
//					int accessnodecount = 0, time_georeach = 0;
//					for(int i = 0;i<al.size();i++)
//					{
//						double x = a_x.get(i);
//						double y = a_y.get(i);
//						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//						
//						System.out.println(i);
//						int id = Integer.parseInt(al.get(i));
//						System.out.println(id);
//						
//						try
//						{
//							georeach.VisitedVertices.clear();
//							long start = System.currentTimeMillis();
//							boolean result3 = georeach.ReachabilityQuery_Bitmap_Total(id, query_rect);
//							time_georeach += System.currentTimeMillis() - start;
//							System.out.println(result3);
//							geoold_result.add(result3);
//							accessnodecount+=georeach.VisitedVertices.size();
//							if(pieces == 128)
//								if(result3)
//									true_count+=1;
//						}
//						catch(Exception e)
//						{
//							e.printStackTrace();
//							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//							i = i-1;
//						}						
//					}
//					OwnMethods.WriteFile(resultpath, true, time_georeach/experiment_node_count+"\t");
//					
//					//GeoReach_New
//					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
//					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//					georeach = new GeoReach_Integrate(rect, pieces);
//					accessnodecount = 0;
//					time_georeach = 0;
//					for(int i = 0;i<al.size();i++)
//					{
//						double x = a_x.get(i);
//						double y = a_y.get(i);
//						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//						
//						System.out.println(i);
//						int id = Integer.parseInt(al.get(i));
//						System.out.println(id);
//						
//						try
//						{
//							georeach.VisitedVertices.clear();
//							long start = System.currentTimeMillis();
//							boolean result3 = georeach.ReachabilityQuery_Bitmap_Partial(id, query_rect);
//							time_georeach += System.currentTimeMillis() - start;
//							System.out.println(result3);
//							geonew_result.add(result3);
//							accessnodecount+=georeach.VisitedVertices.size();
//						}
//						catch(Exception e)
//						{
//							e.printStackTrace();
//							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//							i = i-1;
//						}						
//					}
//					OwnMethods.WriteFile(resultpath, true, time_georeach/experiment_node_count+"\t");
//					
//					//SpaReach
//					int time_spareach = 0;
//					System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//					System.out.println(PostgresJDBC.StopServer());
//					System.out.println(OwnMethods.ClearCache());
//					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//					System.out.println(PostgresJDBC.StartServer());
//					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//					Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
//					for(int i = 0;i<al.size();i++)
//					{
//						double x = a_x.get(i);
//						double y = a_y.get(i);
//						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//						
//						System.out.println(i);
//						int id = Integer.parseInt(al.get(i));
//						System.out.println(id);
//						
//						try
//						{
//							long start = System.currentTimeMillis();
//							boolean result2 = spareach.ReachabilityQuery(id, query_rect);
//							time_spareach += (System.currentTimeMillis() - start);
//							System.out.println(result2);
//							spareach_result.add(result2);
//						}
//						catch(Exception e)
//						{
//							e.printStackTrace();
//							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//							i = i-1;
//						}						
//					}
//					spareach.Disconnect();
//					OwnMethods.WriteFile(resultpath, true, time_spareach/experiment_node_count+"\t");
//				}
//				selectivity*=10;
//				OwnMethods.WriteFile(resultpath, true, true_count+"\n");
//				for(int i = 0;i<experiment_node_count;i++)
//				{
//					if(geonew_result.get(i)!=geoold_result.get(i)||geonew_result.get(i)!=spareach_result.get(i))
//					{
//						System.out.println(al.get(i));
//						System.out.println(a_x.get(i));
//						System.out.println(a_y.get(i));
//						System.out.println(rect_size);
//						isbreak = true;
//						break;
//					}
//				}
//				if(isbreak)
//					break;
//			}
//		}
//		OwnMethods.WriteFile(resultpath, true, "\n");
//	
//	}
//
//}
