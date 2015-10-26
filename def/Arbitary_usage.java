package def;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.neo4j.unsafe.batchinsert.BatchInserter;

public class Arbitary_usage {

	public static void main(String[] args) {
		
		String datasource = "Patents";
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");
		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
		int node_count = OwnMethods.GetNodeCount(datasource);
		int ratio = 60;
		//{
			long offset = ratio / 20 * node_count;
//			try
//			{
//				
//			}
//		String datasource = "Patents";
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		double experiment_node_count = 500;
//		String graph_label = "Graph_Random_80";
//		double spatial_total_range = 1000;
//		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		OwnMethods.GetNodeCount(datasource);
//		
//		Random r = new Random();		
//		double j = 1;
//		while(true)
//		{
//			System.out.println(OwnMethods.ClearCache());
//			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//			Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + 80);
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
//			ArrayList<Double> lx = new ArrayList<Double>();
//			ArrayList<Double> ly = new ArrayList<Double>();
//			
//			int true_count = 0;
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				lx.add(r.nextDouble()*(1000-rect_size));
//				ly.add(r.nextDouble()*(1000-rect_size));
//			}
//			
//			long time = 0;
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				
//				double x = lx.get(i);
//				double y = ly.get(i);
//								
//				MyRectangle rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				try 
//				{
//					long start = System.currentTimeMillis();
//					boolean result = spareach.ReachabilityQuery(id, rect);
//					time += (System.currentTimeMillis() - start);
//					System.out.println(i);
//					System.out.println(result);
//					
//					if(result)
//						true_count+=1;
//				} 
//				catch(Exception e)
//				{
//					e.printStackTrace();
//					i = i-1;
//				}		
//				
//			}
//			
//			spareach.Disconnect();
//			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//			
//			double a_time = time/experiment_node_count;
//			
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/test.txt", true, j+"\t"+a_time+"\t"+true_count+"\n");
//			if(j<1)
//				j*=10;
//			else
//				j+=10;
//			
//			if(j>60)
//				break;
//			
//			if(a_time>3000)
//				break;
//		}
		
		//for(int i = 0;i<1;i++)
			//System.out.println(i);
		
//		double x = 53.283532305734234;
//		double y = 65.15745003013664;
//		
//		MyRectangle spatialrange = new MyRectangle(0,0,1000,1000);
//		double size = 331.66247903553995;
//		GeoReach_Integrate geo = new GeoReach_Integrate(spatialrange, 5);
//		MyRectangle rect = new MyRectangle(x,y,x+size, y+size);
//		System.out.println(geo.ReachabilityQuery(5219357, rect));
		
	}

}
