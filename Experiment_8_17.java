package def;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Experiment_8_17 {
	
	private static long graph_size;
	private static double experiment_node_count = 500.0;
	private static double spatial_total_range = 1000;		

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<String> datasources = new ArrayList<String>();
//		datasources.add("citeseerx");
//		datasources.add("go_uniprot");
		datasources.add("Patents");
		//datasources.add("uniprotenc_22m");
		//datasources.add("uniprotenc_100m");
		//datasources.add("uniprotenc_150m");
		
		for(int datasourcei = 0;datasourcei<datasources.size();datasourcei++)
		{
			String datasource = datasources.get(datasourcei);
			String result_file_path = "/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/query_time.txt";
			boolean break_flag = false;
			graph_size = OwnMethods.GetNodeCount(datasource);
			
			for(int ratio = 20;ratio<100;ratio+=20)
			//int ratio = 80;
			{
				OwnMethods.WriteFile(result_file_path, true, "ratio=" + ratio + "\n");
				OwnMethods.WriteFile(result_file_path, true, "spatial_range\t"+"traversal_time\t"+"GeoReach_time\t"+"GeoReachGrid_time\t"+"true_result_count\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/traversal_time_composition.txt", true, "ratio="+ratio+"\nspatial_range\ttotal_time\tneo4j_time\tjudge_time\ttrue_result_count\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/georeach_time_composition.txt", true, "ratio="+ratio+"\n"+"spatial_range\ttotal_time\tneo4j_time\tjudge_time\ttrue_result_count\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/geogrid_time_composition.txt", true, "ratio="+ratio+"\nspatial_range\ttatal_time\tneo4j_time\tjudge_time\ttrue_result_count\n");
				String graph_label = "Graph_Random_" + ratio;
				HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
				ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
						
				for(int j = 1;j<60;j+=10)
				{
					Traversal traversal = new Traversal();
					GeoReach georeach = new GeoReach();
					MyRectangle rect = new MyRectangle(0,0,1000,1000); 
					Geo_Reach_Grid geogrid = new Geo_Reach_Grid(rect, 5);
					
					double rect_size = spatial_total_range * Math.sqrt(j/100.0);
					Random r = new Random();
					
					long time_traversal = 0,time_georeach = 0, time_geogrid = 0;
					int true_result_count = 0;
					for(int i = 0;i<al.size();i++)
					{
						double x = r.nextDouble()*(1000 - rect_size);
						double y = r.nextDouble()*(1000 - rect_size);
					
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						traversal.VisitedVertices.clear();
						long start = System.currentTimeMillis();
						boolean result1 = traversal.ReachabilityQuery(id, query_rect);
						time_traversal+=System.currentTimeMillis() - start;
						System.out.println(result1);
												
						georeach.VisitedVertices.clear();
						start = System.currentTimeMillis();
						boolean result2 = georeach.ReachabilityQuery(id, query_rect);
						time_georeach+=System.currentTimeMillis() - start;
						System.out.println(result2);
						
						geogrid.VisitedVertices.clear();
						start = System.currentTimeMillis();
						boolean result3 = geogrid.ReachabilityQuery(id, query_rect);
						time_geogrid += System.currentTimeMillis() - start;
						System.out.println(result3);
						
						if(result1!=result2 || result1!=result3)
						{
							System.out.println(ratio);
							System.out.println(id);
							System.out.println(x);
							System.out.println(y);
							System.out.println(rect_size);
							break_flag=true;
							break;
						}
						
						if(result1)
							true_result_count+=1;
					}
					
					

					OwnMethods.WriteFile(result_file_path, true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+time_georeach/experiment_node_count+"\t"+time_geogrid/experiment_node_count+"\t"+true_result_count+"\n");
					
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/traversal_time_composition.txt", true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+traversal.Neo4jTime/experiment_node_count+"\t"+traversal.JudgeTime/experiment_node_count+"\t"+true_result_count+"\n");
									
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/georeach_time_composition.txt", true, (j/100.0)+"\t"+time_georeach/experiment_node_count+"\t"+georeach.neo4j_time/experiment_node_count+"\t"+georeach.judge_time/experiment_node_count+"\t"+true_result_count+"\n");
					
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_5/geogrid_time_composition.txt", true, (j/100.0)+"\t"+time_geogrid/experiment_node_count+"\t"+geogrid.neo4j_time/experiment_node_count+"\t"+geogrid.judge_time/experiment_node_count+"\t"+true_result_count+"\n");

					if(break_flag)
						break;
				}
				if(break_flag)
					break;
				
				OwnMethods.WriteFile(result_file_path, true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/traversal_time_composition.txt", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/georeach_time_composition.txt", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/geogrid_time_composition.txt", true, "\n");
			}
		}				
	}

}
