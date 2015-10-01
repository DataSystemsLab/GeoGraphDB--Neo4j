package def;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class GeoReach_Integrated_test {

	public static void main(String[] args) {
		
		GeoReach_Integrate.Set_Bitmap_Boolean("Patents", 128, 200);
		
//		GeoReach_Integrate.LoadCompresedBitmap(128, "uniprotenc_150m");

//		MyRectangle range = new MyRectangle(0,0,1000,1000);
//		
//		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 128);
//		Traversal tra = new Traversal();
//		
//
////		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 10);
////		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 5);
//		
////		double x = 871.0048749159089;
////		double y = 185.37284713093848;
////		MyRectangle rect = new MyRectangle(x, y, x+100, y+100); 
////		System.out.println(p_geo.ReachabilityQuery(5479369, rect));
//		
//		String datasource = "Patents";
//		//int graph_size = OwnMethods.GetNodeCount(datasource);
//		int graph_size = 3774768;
//		double experiment_node_count = 500;
//		String graph_label = "Graph_Random_80";
//		double spatial_total_range = 1000;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		
//		Random r = new Random();
//		HashMap<Integer, ArrayList<Double>> hs_x = new HashMap();
//		HashMap<Integer, ArrayList<Double>> hs_y = new HashMap();
//		for(int j = 1;j<60;j+=10)
//		{
//			ArrayList<Double> lx = new ArrayList();
//			ArrayList<Double> ly = new ArrayList();
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
//			for(int i = 0;i<experiment_node_count;i++)
//			{
//				double x = r.nextDouble()*(spatial_total_range-rect_size);
//				double y = r.nextDouble()*(spatial_total_range-rect_size);
//				lx.add(x);
//				ly.add(y);
//			}
//			hs_x.put(j, lx);
//			hs_y.put(j, ly);
//		}
//		
//		boolean isbreak = false;
//		int true_count = 0;
//		for(int j = 1;j<60;j+=10)
//		{
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
//			System.out.println(rect_size);
//			ArrayList<Double> lx = hs_x.get(j);
//			ArrayList<Double> ly = hs_y.get(j);
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				double x = lx.get(i);
//				double y = ly.get(i);
//				MyRectangle query_rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				
//				boolean result1 = tra.ReachabilityQuery(id, query_rect);
//				boolean result2 = p_geo.ReachabilityQuery_Bitmap_Partial(id, query_rect);
//				
////				System.out.println(i);
////				System.out.println(id);
////				System.out.println(result1);
////				System.out.println(result2);
//				
//				if(result1)
//					true_count+=1;
//				
//				if(result1!=result2)
//				{
//					System.out.println(i);
//					System.out.println(x);
//					System.out.println(y);
//					System.out.println(rect_size);
//					System.out.println(true_count);
//					
//					isbreak = true;
//					break;
//				}
//			}
//			
//			System.out.println(true_count);
//			
//			if(isbreak)
//				break;
//		}
		
	}

}
