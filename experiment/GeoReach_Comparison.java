package experiment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import def.*;

public class GeoReach_Comparison {

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
			String result_file_path = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time.csv";
			boolean break_flag = false;
			graph_size = OwnMethods.GetNodeCount(datasource);
			
			for(int ratio = 20;ratio<100;ratio+=20)
			//int ratio = 80;
			{
				OwnMethods.WriteFile(result_file_path, true, "ratio=" + ratio + "\n");
				OwnMethods.WriteFile(result_file_path, true, "spatial_range\t"+"traversal_time\t"+"SpatialIndex_time\tSpatialReachability_time\t"+"GeoReach_5_time\t"+"GeoReach_10\t"+"true_result_count\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_5_composition.csv", true, "ratio="+ratio+"\nspatial_range\t"+"total_time\t"+"neo4j_time\t"+"judge_time\t"+"true_result_count\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_10_composition.csv", true, "ratio="+ratio+"\nspatial_range\t"+"total_time\t"+"neo4j_time\t"+"judge_time\t"+"true_result_count\n");
				
				String graph_label = "Graph_Random_" + ratio;
					
				System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
				HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
				ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
				System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
				
				ArrayList<Double> a_x = new ArrayList<Double>();
				ArrayList<Double> a_y = new ArrayList<Double>();
				
				Random r = new Random();
				
				double j = 0.01;
				while(true)
				{
					
					MyRectangle rect = new MyRectangle(0,0,1000,1000);
									
					double rect_size = spatial_total_range * Math.sqrt(j/100.0);	
					
					a_x.clear();
					a_y.clear();
					for(int i = 0;i<experiment_node_count;i++)
					{
						a_x.add(r.nextDouble()*(1000-rect_size));
						a_y.add(r.nextDouble()*(1000-rect_size));
					}
										
					long time_traversal = 0,time_georeach_5 = 0, time_georeach_10 = 0, time_spa = 0, time_spareach = 0;
					int true_result_count_5 = 0,true_result_count_10 = 0;									
					
					//georeach_5
					System.out.println(OwnMethods.ClearCache());
					System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
					ArrayList<Boolean> result_5 = new ArrayList<Boolean>();
					GeoReach_Integrate georeach_5 = new GeoReach_Integrate(rect, 5);
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
							georeach_5.VisitedVertices.clear();
							long start = System.currentTimeMillis();
							boolean result3 = georeach_5.ReachabilityQuery(id, query_rect);
							time_georeach_5 += System.currentTimeMillis() - start;
							System.out.println(result3);
							result_5.add(result3);
							if(result3)
								true_result_count_5+=1;
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
						}
												
					}	
					
					//georeach_10
					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
					GeoReach_Integrate georeach_10 = new GeoReach_Integrate(rect, 10);
					ArrayList<Boolean> result_10 = new ArrayList<Boolean>();
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
							georeach_10.VisitedVertices.clear();
							long start = System.currentTimeMillis();
							boolean result3 = georeach_10.ReachabilityQuery(id, query_rect);
							time_georeach_10 += System.currentTimeMillis() - start;
							System.out.println(result3);
							result_10.add(result3);
							if(result3)
								true_result_count_10+=1;
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
						}
												
					}					
					
					System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
									
					OwnMethods.WriteFile(result_file_path, true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+time_spa/experiment_node_count+"\t"+time_spareach/experiment_node_count+"\t"+time_georeach_5/experiment_node_count+"\t"+time_georeach_10/experiment_node_count+"\t"+true_result_count_5+"\n");
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_5_composition.csv", true, (j/100.0)+"\t"+time_georeach_5/experiment_node_count+"\t"+georeach_5.neo4j_time/experiment_node_count+"\t"+georeach_5.judge_time/experiment_node_count+"\t"+true_result_count_5+"\n");
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_10_composition.csv", true, (j/100.0)+"\t"+time_georeach_10/experiment_node_count+"\t"+georeach_10.neo4j_time/experiment_node_count+"\t"+georeach_10.judge_time/experiment_node_count+"\t"+true_result_count_10+"\n");
					
					
					for(int i = 0;i<result_5.size();i++)
						if(result_5.get(i)!=result_10.get(i))
						{
							System.out.println(i);
							System.out.println(al.get(i));
							System.out.println(a_x.get(i));
							System.out.println(a_y.get(i));
							System.out.println(rect_size);
							break_flag = true;
							break;
						}
						
								
					if(break_flag)
						break;
					
					if(j<1)
						j*=10;
					else
						j+=10;
					
					if(j>60)
						break;
				}
				if(break_flag)
					break;
				
				OwnMethods.WriteFile(result_file_path, true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_5_composition.csv", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/grid_10_composition.csv", true, "\n");
			}
			if(break_flag)
				break;
		}	
		
	}

}
