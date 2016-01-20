package def;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class GeoReach_test 
{	
	public static void Experiment()
	{
		String datasource = "Patents";
		int graph_size = OwnMethods.GetNodeCount(datasource);
//		int graph_size = 3774768;
		int experiment_node_count = 50;
		MyRectangle range = new MyRectangle(0,0,1000,1000);
		double spatial_total_range = 1000;
		HashSet<Long> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
		ArrayList<Long> al = new ArrayList<Long>();
		Iterator<Long> iter = hs.iterator();
		while(iter.hasNext())
			al.add(iter.next()+graph_size);
		
		Random r = new Random();
		HashMap<Integer, ArrayList<Double>> hs_x = new HashMap<Integer, ArrayList<Double>>();
		HashMap<Integer, ArrayList<Double>> hs_y = new HashMap<Integer, ArrayList<Double>>();
		for(int j = 21;j<60;j+=10)
		{
			ArrayList<Double> lx = new ArrayList<Double>();
			ArrayList<Double> ly = new ArrayList<Double>();
			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
			for(int i = 0;i<experiment_node_count;i++)
			{
				double x = r.nextDouble()*(spatial_total_range-rect_size);
				double y = r.nextDouble()*(spatial_total_range-rect_size);
				lx.add(x);
				ly.add(y);
			}
			hs_x.put(j, lx);
			hs_y.put(j, ly);
		}
		
		boolean isbreak = false;
		for(int j = 21;j<60;j+=10)
		{
			int true_count = 0;
			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
			System.out.println(rect_size);
			ArrayList<Double> lx = hs_x.get(j);
			ArrayList<Double> ly = hs_y.get(j);
			
//			GeoReach_Integrate p_geo_full =  new GeoReach_Integrate(range, 128);
//			GeoReach_Integrate p_geo_multi = new GeoReach_Integrate(range, 128);
			GeoReach p_geo = new GeoReach(range, 128);
			Traversal tra = new Traversal();
			
			for(int i = 0;i<al.size();i++)
			{
				long id = al.get(i);
				double x = lx.get(i);
				double y = ly.get(i);
				MyRectangle query_rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
				
//				boolean result1 = tra.ReachabilityQuery(id, query_rect);
//				boolean result2 = p_geo.ReachabilityQuery_Bitmap_Partial(id, query_rect);
				
//				boolean result1 = p_geo_full.ReachabilityQuery_FullGrids(id, query_rect);
//				boolean result2 = p_geo_multi.ReachabilityQuery_Bitmap_MultiResolution(id, query_rect,2);
				
				boolean result1 = tra.ReachabilityQuery(id, query_rect);
				boolean result2 = p_geo.ReachabilityQuery(id, query_rect);
				
				
//				System.out.println(i);
//				System.out.println(id);
				System.out.println(result1);
				System.out.println(result2);
				
				if(result1)
					true_count+=1;
				
				if(result1!=result2)
				{
					System.out.println(i);
					System.out.println(id);
					System.out.println(x);
					System.out.println(y);
					System.out.println(rect_size);
					System.out.println(true_count);
					
					isbreak = true;
					break;
				}
			}
			
			System.out.println(true_count);
			
			if(isbreak)
				break;
		}
	}
	
	public static void main(String[] args)
	{
//		MyRectangle range = new MyRectangle(0,0,1000,1000);
//		GeoReach p_geo = new GeoReach(range, 128);
//		
//		double size = 458.257569495584;
//		double x = 440.63804988348716, y = 122.11543244228389;
//		MyRectangle rect = new MyRectangle(x, y, x+size, y+size);
//		System.out.println(p_geo.ReachabilityQuery(5041870, rect));
		Experiment();
//		String bitmap = "OjAAAAEAAAAAAGYAEAAAACUAmgBZAWkBbwE0AjYCRQO4A7oDewQUBTkF5wVaBq8G/gYgB1wHMgleCacJ6Qr1DWwPxw9LEMkQjROtE+sTrBQRFR4VPhWnFWAWqReyGBkZSRnKGRobvBs4HMAeKx8wH7QfvyDQIAohtSGmIjEjYiOUIwImVSa5JuYmDScrJ2EoQCn9KZUq1SoQK7gr4SuqLBQtJi0DLlYuYi+YMdQyQzPIMzw0ODVPNq827Tb+N6E5cjrEOh47xjz1PLA9uj2/PeA97T1SPnk+mT/kPxtJ";
//		System.out.println(OwnMethods.Deserialize_String_ToRoarBitmap(bitmap));
	}
}
