package experiment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import def.GeoReach_Integrate;
import def.MyRectangle;
import def.Neo4j_Graph_Store;
import def.OwnMethods;

public class Experiment_8_29 {
	
	private static long graph_size;
	private static double experiment_node_count = 500.0;
	private static double spatial_total_range = 1000;

	public static void main(String[] args) 
	{
		String datasource = "Patents";
		String resultpath = "/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_29.csv";
		OwnMethods.WriteFile(resultpath, true, "spatial_range\t");
		for(int pieces = 128;pieces>=64;pieces/=2)
		{
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/query_time_8_29.csv", true, "GeoReach_"+pieces+"\tTraversal\tInMemoryJudge\tAccessNodeCount\tNeighborOperationCount\tfalse_in\tfalse_out\tfalse_all\t");
		}
		OwnMethods.WriteFile(resultpath, true, "true_count\n");
		double selectivity = 0.91;
		int ratio = 80;
		String graph_label = "Graph_Random_" + ratio;
		graph_size = OwnMethods.GetNodeCount(datasource);
		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
		
		ArrayList<Double> a_x = new ArrayList<Double>();
		ArrayList<Double> a_y = new ArrayList<Double>();
		
		Random r = new Random();
		a_x.clear();
		a_y.clear();
		for(int i = 0;i<experiment_node_count;i++)
		{
			a_x.add(r.nextDouble()*(1000));
			a_y.add(r.nextDouble()*(1000));
		}
		
		{
			while(selectivity>=0.0000001)
			{
				double rect_size = spatial_total_range * Math.sqrt(selectivity);
				OwnMethods.WriteFile(resultpath, true, selectivity+"\t");
				
//				a_x.clear();
//				a_y.clear();
//				for(int i = 0;i<experiment_node_count;i++)
//				{
//					a_x.add(r.nextDouble()*(1000-rect_size));
//					a_y.add(r.nextDouble()*(1000-rect_size));
//				}
				
				int true_count = 0;
				for(int pieces = 128;pieces>=64;pieces/=2)
				{
					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
					MyRectangle rect = new MyRectangle(0,0,1000,1000); 
					GeoReach_Integrate georeach = new GeoReach_Integrate(rect, pieces);
					int accessnodecount = 0, time_georeach = 0;
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
							boolean result3 = georeach.ReachabilityQuery(id, query_rect);
							time_georeach += System.currentTimeMillis() - start;
							System.out.println(result3);
							accessnodecount+=georeach.VisitedVertices.size();
							if(pieces == 128)
								if(result3)
									true_count+=1;
						}
						catch(Exception e)
						{
							e.printStackTrace();
							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
							i = i-1;
						}						
					}
					OwnMethods.WriteFile(resultpath, true, time_georeach/experiment_node_count+"\t"+georeach.neo4j_time/experiment_node_count+"\t"+georeach.judge_time/experiment_node_count+"\t"+accessnodecount/experiment_node_count+"\t"+georeach.Neo4jAccessCount/experiment_node_count+"\t"+georeach.false_inside+"\t"+georeach.false_outside+"\t"+georeach.false_all+"\t");
				}
				if(selectivity<=0.01)
					selectivity/=10;
				else
					if(Math.abs((selectivity - 0.11))<=0.000000001)
						selectivity-=0.1;
					else
						selectivity-=0.4;
				OwnMethods.WriteFile(resultpath, true, true_count+"\n");
			}
		}
		OwnMethods.WriteFile(resultpath, true, "\n");
		
		
		
		
	}

}
