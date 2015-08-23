package def;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Experiment_8_17 {
	
	private static long graph_size;
	private static double experiment_node_count = 500.0;
	private static double spatial_total_range = 1000;		

	public static void main(String[] args) {
		ArrayList<String> datasources = new ArrayList<String>();
//		datasources.add("citeseerx");
//		datasources.add("uniprotenc_22m");
		datasources.add("Patents");
//		datasources.add("uniprotenc_22m");
//		datasources.add("uniprotenc_100m");
//		datasources.add("uniprotenc_150m");
		
		for(int datasourcei = 0;datasourcei<datasources.size();datasourcei++)
		{
			String datasource = datasources.get(datasourcei);
			String result_file_path = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time.txt";
			boolean break_flag = false;
			graph_size = OwnMethods.GetNodeCount(datasource);
			
			for(int ratio = 20;ratio<100;ratio+=20)
			//int ratio = 80;
			{
				OwnMethods.WriteFile(result_file_path, true, "ratio=" + ratio + "\n");
				OwnMethods.WriteFile(result_file_path, true, "spatial_range\t"+"traversal_time\t"+"SpatialIndex_time\tSpatialReachability_time\t"+"GeoReach_time\t"+"GeoReachGrid_time\t"+"true_result_count\n");
								
				String graph_label = "Graph_Random_" + ratio;
					
				System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
				HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
				ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
				System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
				
				ArrayList<Double> a_x = new ArrayList<Double>();
				ArrayList<Double> a_y = new ArrayList<Double>();
				
				Random r = new Random();
				
				boolean run_spa = true, run_spareach = true;
				
				for(int j = 1;j<60;j+=10)
				{
					Traversal traversal = new Traversal();
					GeoReach georeach = new GeoReach();
					
					MyRectangle rect = new MyRectangle(0,0,1000,1000); 
					Geo_Reach_Grid geogrid = new Geo_Reach_Grid(rect, 5);
					
					Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
					SpatialIndex spa = new SpatialIndex(datasource + "_Random_" + ratio);
					
					double rect_size = spatial_total_range * Math.sqrt(j/100.0);	
					
					a_x.clear();
					a_y.clear();
					for(int i = 0;i<experiment_node_count;i++)
					{
						a_x.add(r.nextDouble()*(1000-rect_size));
						a_y.add(r.nextDouble()*(1000-rect_size));
					}
										
					long time_traversal = 0,time_georeach = 0, time_geogrid = 0, time_spa = 0, time_spareach = 0;
					int true_result_count = 0;
					
					//traveral
					System.out.println(OwnMethods.ClearCache());
					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
					for(int i = 0;i<al.size();i++)
					{
						double x = a_x.get(i);
						double y = a_y.get(i);
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						try
						{
							traversal.VisitedVertices.clear();
							long start = System.currentTimeMillis();
							boolean result1 = traversal.ReachabilityQuery(id, query_rect);
							time_traversal+=System.currentTimeMillis() - start;
							System.out.println(result1);

							if(result1)
								true_result_count+=1;	
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
							continue;
						}
											
					}	
					
					//georeach
					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
					for(int i = 0;i<al.size();i++)
					{
						double x = a_x.get(i);
						double y = a_y.get(i);
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						try
						{
							georeach.VisitedVertices.clear();
							long start = System.currentTimeMillis();
							boolean result2 = georeach.ReachabilityQuery(id, query_rect);
							time_georeach+=System.currentTimeMillis() - start;
							System.out.println(result2);
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
							continue;
						}
						
					}					
					
					//geogrid
					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));									
					for(int i = 0;i<al.size();i++)
					{
						double x = a_x.get(i);
						double y = a_y.get(i);
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						try
						{
							geogrid.VisitedVertices.clear();
							long start = System.currentTimeMillis();
							boolean result3 = geogrid.ReachabilityQuery(id, query_rect);
							time_geogrid += System.currentTimeMillis() - start;
							System.out.println(result3);
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
							continue;
						}
												
					}	
					
					if(run_spa)
					{
						//spatial index
						System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
						for(int i = 0;i<al.size();i++)
						{
							double x = a_x.get(i);
							double y = a_y.get(i);
							MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
							
							System.out.println(i);
							int id = Integer.parseInt(al.get(i));
							System.out.println(id);
							
							
							try 
							{
								long start = System.currentTimeMillis();
								boolean result4 = spa.ReachabilityQuery(id, query_rect);
								time_spa += System.currentTimeMillis() - start;
								System.out.println(result4);
							} 
							catch(Exception e)
							{
								e.printStackTrace();
								OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
								i = i-1;
								continue;
							}
						}
					}
					
					if(run_spareach)
					{
						//spatial and reachability index
						System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));									
						for(int i = 0;i<al.size();i++)
						{
							double x = a_x.get(i);
							double y = a_y.get(i);
							MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
							
							System.out.println(i);
							int id = Integer.parseInt(al.get(i));
							System.out.println(id);
							
							try 
							{
								long start = System.currentTimeMillis();
								boolean result2 = spareach.ReachabilityQuery(id, query_rect);
								time_spareach += (System.currentTimeMillis() - start);
								System.out.println(result2);
							} 
							catch(Exception e)
							{
								e.printStackTrace();
								OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
								i = i-1;
								continue;
							}				
						}	
					}
					
					System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
					
					if(time_spa/experiment_node_count>1500)
						run_spa = false;
					
					if(time_spareach/experiment_node_count>1500)
						run_spareach = false;
									
					OwnMethods.WriteFile(result_file_path, true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+time_spa/experiment_node_count+"\t"+time_spareach/experiment_node_count+"\t"+time_georeach/experiment_node_count+"\t"+time_geogrid/experiment_node_count+"\t"+true_result_count+"\n");
										
					if(break_flag)
						break;
				}
				if(break_flag)
					break;
				
				OwnMethods.WriteFile(result_file_path, true, "\n");
			}
			if(break_flag)
				break;
		}	
		
	}

}
