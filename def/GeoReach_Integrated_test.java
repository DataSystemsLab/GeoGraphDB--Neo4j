package def;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class GeoReach_Integrated_test {

	public static void main(String[] args) {
		
		String datasource = "citeseerx";
		Traversal tra = new Traversal();
		int ratio = 20;
//		int id = 12132840;
		MyRectangle range_rect = new MyRectangle(0,0,1000,1000);
		GeoReach_Integrate p_geo = new GeoReach_Integrate(range_rect, 128);
		double x = 264.46316069473073, y = 802.0735049680968, size = 31.622776601683793;
		MyRectangle rect = new MyRectangle(x,y,x+size, y+size);
//		System.out.println(tra.ReachabilityQuery(id, rect, ratio));
//		System.out.println(p_geo.ReachabilityQuery_FullGrids(id, rect));
//		System.out.println(p_geo.ReachabilityQuery_Bitmap_MultiResolution(id, rect, 2));
		//System.out.println(p_geo.ReachabilityQuery_Bitmap_MultiResolution(id, rect, 3));
		
		ArrayList<Long> al = OwnMethods.ReadExperimentNode(datasource);
		for(int i = 0;i<al.size();i++)
		{
			System.out.println(p_geo.ReachabilityQuery_Bitmap_Partial(al.get(i), rect));
		}
		
//		int ratio = 20;
//
//		MyRectangle range = new MyRectangle(0,0,1000,1000);
//		
//		GeoReach_Integrate p_geo = new GeoReach_Integrate(range, 128, ratio, "");		
//		String datasource = "Patents";
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		double experiment_node_count = 500;
//		double spatial_total_range = 1000;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<Integer> al = new ArrayList<Integer>();
//		Iterator<String> iter = hs.iterator();
//		while(iter.hasNext())
//			al.add((Integer.parseInt(iter.next())+graph_size));
//		
//		Random r = new Random();
//		HashMap<Integer, ArrayList<Double>> hs_x = new HashMap<Integer, ArrayList<Double>>();
//		HashMap<Integer, ArrayList<Double>> hs_y = new HashMap<Integer, ArrayList<Double>>();
//		for(int j = 1;j<60;j+=10)
//		{
//			ArrayList<Double> lx = new ArrayList<Double>();
//			ArrayList<Double> ly = new ArrayList<Double>();
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
//				int id = al.get(i);
//				double x = lx.get(i);
//				double y = ly.get(i);
//				MyRectangle query_rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				
////				boolean result1 = tra.ReachabilityQuery(id, query_rect);
//				boolean result1 = p_geo.ReachabilityQuery_Bitmap_MultiResolution(id, query_rect, 2);
//				boolean result2 = p_geo.ReachabilityQuery_FullGrids(id, query_rect);
//				boolean result3 = p_geo.ReachabilityQuery_Bitmap_MultiResolution(id, query_rect, 3);
//				System.out.println(i);
////				System.out.println(id);
//				System.out.println(result1);
////				System.out.println(result2);
//				
//				if(result1)
//					true_count+=1;
//				
//				if(result1!=result2||result1!=result3)
//				{
//					System.out.println(result1);
//					System.out.println(result2);
//					System.out.println(result3);
//					System.out.println(id);
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
