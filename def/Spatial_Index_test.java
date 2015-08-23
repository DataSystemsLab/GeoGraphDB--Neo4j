package def;

import java.util.ArrayList;
import java.util.HashSet;

public class Spatial_Index_test {

	public static void main(String[] args) {
		
		//SpatialIndex.Construct_RTree_Index("citeseer");
		
		ArrayList<String> datasources = new ArrayList<String>();
		datasources.add("citeseerx");
		datasources.add("go_uniprot");
//		datasources.add("uniprotenc_22m");
		datasources.add("uniprotenc_100m");
		datasources.add("uniprotenc_150m");
		for(int i = 0;i<datasources.size();i++)
//		int i = 2;
		{
			String datasource = datasources.get(i);
//			SpatialIndex.DropTable(datasource);
//			SpatialIndex.Construct_RTree_Index(datasource);
			SpatialIndex.CreateTable(datasource);
			SpatialIndex.LoadData(datasource);
			SpatialIndex.CreateGistIndex(datasource);
		}
		
		
		
		// TODO Auto-generated method stub
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
