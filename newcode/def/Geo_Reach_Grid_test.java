package def;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Geo_Reach_Grid_test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Geo_Reach_Grid.LoadIndex(128, "Patents");
//		long size = OwnMethods.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db"));
		Geo_Reach_Grid.LoadBitmapIndex(128, "Patents");
//		size = OwnMethods.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db"))-size;
//		System.out.println(size);
//		String datasource = "Patents";
//		String graph_label = "Graph_Random_20";
//		int experiment_node_count = 100;
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		
//		MyRectangle rect = new MyRectangle(0,0,1000,1000);
//		Geo_Reach_Grid p_geogrid = new Geo_Reach_Grid(rect, 5);
//		Traversal tra = new Traversal();
//		
//		Random r = new Random();
//		
//		for(int j = 10;j<600;j+=100)
//		{
//			OwnMethods.WriteFile("/home/yuhansun/Documents/time.txt", true, ""+j+"\n");
//			
//			long time1 = 0, time2 = 0;
//			for(int i = 0;i<al.size();i++)
//			{
//				double x = r.nextDouble()*(1000 - j);
//				double y = r.nextDouble()*(1000 - j);
//				MyRectangle query_rect = new MyRectangle(x, y, x + j, y + j);
//				
//				int id = Integer.parseInt(al.get(i));
//				System.out.println(id);
//				long start = System.currentTimeMillis();
//				boolean result1 = tra.ReachabilityQuery(id, query_rect);
//				time1+=System.currentTimeMillis() - start;
//				System.out.println(result1);
//				
//				p_geogrid.VisitedVertices.clear();
//				start = System.currentTimeMillis();
//				boolean result2 = p_geogrid.ReachabilityQuery(id, query_rect);	
//				time2 += System.currentTimeMillis() - start;
//				System.out.println(result2);
//				if(result1!=result2)
//				{
//					System.out.println(id);
//					System.out.println(x);
//					System.out.println(y);
//					System.out.println(j);
//					break;
//				}
//			}
//			OwnMethods.WriteFile("/home/yuhansun/Documents/time.txt", true, ""+time1+"\t"+time2+"\t"+"\n");
//		}
		
		
//		MyRectangle rect = new MyRectangle(0,0,1000,1000);
//		Geo_Reach_Grid p_geogrid = new Geo_Reach_Grid(rect, 5);
//		MyRectangle queryrect = new MyRectangle(171.3811593990689,308.22763817535485,171.3811593990689+510,308.22763817535485+510);
//		System.out.println(p_geogrid.ReachabilityQuery(7317282, queryrect));
		
		
	}

}
