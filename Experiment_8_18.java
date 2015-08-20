package def;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Experiment_8_18 {
	
	private static long graph_size;
	private static double experiment_node_count = 500.0;
	private static double spatial_total_range = 1000;

	public static void main(String[] args) {
	
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
			String result_file_path = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time.txt";
			graph_size = OwnMethods.GetNodeCount(datasource);
			
			for(int ratio = 20;ratio<100;ratio+=20)
//			int ratio = 20;
			{
				OwnMethods.WriteFile(result_file_path, true, "ratio=" + ratio + "\n");
				OwnMethods.WriteFile(result_file_path, true, "spatial_range\t"+"traversal_time\t"+"SpatialIndex_time\t"+"SpatialReachIndex_time\t"+"GeoReach_time\tGeoReachGrid_time\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/traversal_time_composition.txt", true, "ratio="+ratio+"\nspatial_range\ttotal_time\tneo4j_time\tjudge_time\n");

				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialindex_time_composition.txt", true, "ratio="+ratio+"\n"+"spatial_range\ttotal_time\tneo4j_time\tpostgres_time\tjudge_time\n");

				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialreachindex_time_composition.txt", true, "ratio="+ratio+"\n"+"spatial_range\ttotal_time\tneo4j_time\tpostgres_time\tjudge_time\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/georeach_time_composition.txt", true, "ratio="+ratio+"\n"+"spatial_range\ttotal_time\tneo4j_time\tjudge_time\n");
				
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/reachgrid_time_composition.txt", true, "ratio="+ratio+"\n"+"spatial_range\ttotal_time\tneo4j_time\tjudge_time\n");
				
				String graph_label = "Graph_Random_" + ratio;
				String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/Random_spatial_distributed/" + ratio;
				HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
				ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
				OwnMethods.WriteFile(filepath + "/experiment_node.txt", true, "ratio="+ratio+"\n");
				OwnMethods.WriteFile(filepath + "/experiment_node.txt", true, al);
				
				OwnMethods.WriteFile(filepath + "/experiment_rectangle_location.txt", true, "ratio="+ratio+"\n");
						
				for(int j = 1;j<60;j+=10)
//				int j = 1;
				{
					Traversal traversal = new Traversal();
					Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
					GeoReach georeach = new GeoReach();
					SpatialIndex spa = new SpatialIndex(datasource + "_Random_" + ratio);
					
					double rect_size = spatial_total_range * Math.sqrt(j/100.0);
					OwnMethods.WriteFile(filepath + "/experiment_rectangle_location.txt", true, "spatial_range="+j/100.0+"\n");
					
					Random r = new Random();
					ArrayList<Double> a_x = new ArrayList<Double>();
					ArrayList<Double> a_y = new ArrayList<Double>();
					for(int i = 0;i<al.size();i++)
					{
						double x = r.nextDouble()*(1000-rect_size);
						double y = r.nextDouble()*(1000-rect_size);
						a_x.add(x);
						a_y.add(y);
					}
					
					long time_traversal = 0,time_reachindex = 0,time_georeach = 0,time_spa = 0,time_reachgrid = 0;
					int true_result_count = 0;
					for(int i = 0;i<al.size();i++)
					{
						double x = a_x.get(i);
						double y = a_y.get(i);					
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						traversal.VisitedVertices.clear();
						long start = System.currentTimeMillis();
						boolean result1 = traversal.ReachabilityQuery(id, query_rect);
						time_traversal+=System.currentTimeMillis() - start;
						System.out.println(result1);
						
						if(result1)
							true_result_count+=1;
					}
					
					for(int i = 0;i<al.size();i++)
					{						
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						double x = a_x.get(i);
						double y = a_y.get(i);					
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						long start = System.currentTimeMillis();
						boolean result4 = spa.ReachabilityQuery(id, query_rect);
						time_spa += System.currentTimeMillis() - start;
						System.out.println(result4);
					}
					
					for(int i = 0;i<al.size();i++)
					{	
						System.out.println(i);
						int id = Integer.parseInt(al.get(i));
						System.out.println(id);
						
						double x = a_x.get(i);
						double y = a_y.get(i);					
						MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
						
						long start = System.currentTimeMillis();
						boolean result2 = spareach.ReachabilityQuery(id, query_rect);
						time_reachindex+= (System.currentTimeMillis() - start);
						System.out.println(result2);
						
						georeach.VisitedVertices.clear();
						start = System.currentTimeMillis();
						boolean result3 = georeach.ReachabilityQuery(id, query_rect);
						time_georeach+=System.currentTimeMillis() - start;
						System.out.println(result3);
					}
					
					OwnMethods.WriteFile(filepath + "//experiment_node.txt", true, "\n");
					OwnMethods.WriteFile(filepath + "/experiment_rectangle_location.txt", true, "\n");
					

					OwnMethods.WriteFile(result_file_path, true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+time_spa/experiment_node_count+"\t"+time_reachindex/experiment_node_count+"\t"+time_georeach/experiment_node_count+"\t"+true_result_count+"\n");
					
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/traversal_time_composition.txt", true, (j/100.0)+"\t"+time_traversal/experiment_node_count+"\t"+traversal.Neo4jTime/experiment_node_count+"\t"+traversal.JudgeTime/experiment_node_count+"\t"+true_result_count+"\n");
					
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialindex_time_composition.txt", true, (j/100.0)+"\t"+time_spa/experiment_node_count+"\t"+spa.Neo4jTime/experiment_node_count+"\t"+spa.PostgresTime/experiment_node_count+"\t"+spa.JudgeTime/experiment_node_count+"\t"+true_result_count+"\n");
					
					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialreachindex_time_composition.txt", true, (j/100.0)+"\t"+time_reachindex/experiment_node_count+"\t"+spareach.neo4j_time/experiment_node_count+"\t"+spareach.postgresql_time/experiment_node_count+"\t"+spareach.judge_time/experiment_node_count+"\t"+true_result_count+"\n");

					OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/georeach_time_composition.txt", true, (j/100.0)+"\t"+time_georeach/experiment_node_count+"\t"+georeach.neo4j_time/experiment_node_count+"\t"+georeach.judge_time/experiment_node_count+"\t"+true_result_count+"\n");
					
					spareach.Disconnect();
					spa.Disconnect();

				}
				
				OwnMethods.WriteFile(filepath + "/experiment_rectangle_location.txt", true, "\n");
				OwnMethods.WriteFile(result_file_path, true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/traversal_time_composition.txt", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialindex_time_composition.txt", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/spatialreachindex_time_composition.txt", true, "\n");
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/georeach_time_composition.txt", true, "\n");
			}
		}	
	}

}
