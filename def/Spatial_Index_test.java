package def;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Spatial_Index_test {

	private static long graph_size;
	private static double experiment_node_count = 50.0;
	private static double spatial_total_range = 1000;
	
	public static void main(String[] args) {
		
		String datasource = "Patents";
		
		for(int ratio = 20;ratio<80;ratio+=20)
		{
			String graph_label = "Graph_Random_" + ratio;
			
			graph_size = OwnMethods.GetNodeCount(datasource);
			
			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
			HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
			ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
			
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/augment.csv", true, "ratio="+ratio+"\n");
			
			ArrayList<Double> a_x = new ArrayList<Double>();
			ArrayList<Double> a_y = new ArrayList<Double>();
			
			Random r = new Random();
			
			for(int j = 1;j<60;j+=10)
			{
				double rect_size = spatial_total_range * Math.sqrt(j/100.0);
				
				a_x.clear();
				a_y.clear();
				for(int i = 0;i<experiment_node_count;i++)
				{
					a_x.add(r.nextDouble()*(1000-rect_size));
					a_y.add(r.nextDouble()*(1000-rect_size));
				}
				
				long time_spa = 0, time_spareach = 0;
				
				System.out.println(OwnMethods.ClearCache());
				System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
				SpatialIndex spa = new SpatialIndex(datasource + "_Random_" + ratio);
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
					}
				}
				
//				if(j>0.01)
//				{
//					System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));	
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
//						} 
//						catch(Exception e)
//						{
//							e.printStackTrace();
//							OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//							i = i-1;
//						}				
//					}
//					
//					
//				}
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/augment.csv", true,j/100.0 + "\t"+ time_spa/experiment_node_count+"\t"+time_spareach/experiment_node_count+"\n");
				Neo4j_Graph_Store.StopMyServer(datasource);
			}
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/augment.csv", true, "\n");
		}
		
		//SpatialIndex.Construct_RTree_Index("citeseer");
		
//		ArrayList<String> datasources = new ArrayList<String>();
//		datasources.add("citeseerx");
//		datasources.add("go_uniprot");
//		datasources.add("uniprotenc_22m");
//		datasources.add("uniprotenc_100m");
//		datasources.add("uniprotenc_150m");
//		for(int i = 0;i<datasources.size();i++)
//		int i = 2;
//		{
//			String datasource = datasources.get(i);
////			SpatialIndex.DropTable(datasource);
////			SpatialIndex.Construct_RTree_Index(datasource);
//			SpatialIndex.CreateTable(datasource);
//			SpatialIndex.LoadData(datasource);
//			SpatialIndex.CreateGistIndex(datasource);
//		}
		
		
		
//		String graph_label = "Graph_Random_20";
//		String RTree_name = "Patents_Random_20";
//		OwnMethods own = new OwnMethods();
//		HashSet<String> hs = own.GenerateRandomInteger(3774768, 100);
//		ArrayList<String> al = own.GenerateStartNode(hs, graph_label);
//		own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/test_absolute_id", true, al);
//		
//		for(int size = 10;size<810;size+=100)
//		{
//			MyRectangle rect = new MyRectangle(0,0,size,size);
//			SpatialIndex spa = new SpatialIndex(RTree_name);
//			Traversal tra = new Traversal();		
//					
//			long time_tra = 0, time_spa = 0;
//			
////			for(int i = 0;i<al.size();i++)
////			{
////				int id = Integer.parseInt(al.get(i));
////				System.out.println(i);
////				
////				long start = System.currentTimeMillis();
////				boolean result_spa = spa.ReachabilityQuery(id, rect);
////				time_spa += System.currentTimeMillis() - start;
////				System.out.println(result_spa);
////				
////				start = System.currentTimeMillis();
////				boolean result_tra = tra.ReachabilityQuery(id, rect);
////				time_tra+=System.currentTimeMillis() - start;
////				System.out.println(result_tra);
////				
////				if(result_tra!=result_spa)
////				{
////					System.out.println(id);
////					break;
////				}
////			}
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				System.out.println(i);
//				
//				long start = System.currentTimeMillis();
//				boolean result_tra = tra.ReachabilityQuery(id, rect);
//				time_tra+=System.currentTimeMillis() - start;
//			}
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				System.out.println(i);
//				
//				long start = System.currentTimeMillis();
//				boolean result_spa = spa.ReachabilityQuery(id, rect);
//				time_spa += System.currentTimeMillis() - start;
//				
//			}
//			
//			
//			
//			own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/traversal_spatial_comparison.txt", true,""+size+"\n");
//			own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/traversal_spatial_comparison.txt", true, "tra_time: "+time_tra+"\tNeo4jTime: "+tra.Neo4jTime+"\tJudgeTime: "+tra.JudgeTime+"\n");
//			own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/traversal_spatial_comparison.txt", true, "spa_time: "+time_spa+"\tNeo4jTime: "+spa.Neo4jTime+"\tJudgeTime: "+spa.JudgeTime+"\tPostTime: "+spa.PostgresTime+"\n");
//		}
//		
		
	}

}
