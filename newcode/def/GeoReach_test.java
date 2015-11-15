package def;

import java.util.*;

public class GeoReach_test 
{	
	private static GeoReach p_georeach = new GeoReach("_random", 20);
	
	public static void main(String[] args)
	{
		/*Rectangle rect = p_georeach.GetRMBR(17585);
		System.out.println(rect.min_x);
		System.out.println(rect.min_y);
		System.out.println(rect.max_x);
		System.out.println(rect.max_y);*/
		
		/*long start = System.currentTimeMillis();
		p_georeach.Preprocess();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		
		double x = 333.6805821379945, y = 847.520696027844;
		
		MyRectangle rect = new MyRectangle(x,y,x+10.0,y+10.0);
		System.out.println(p_georeach.ReachabilityQuery(12132840, rect));
		/*long sumtime = 0;
		for(int i = 0;i<50;i++)
		{
			long start = System.currentTimeMillis();
			System.out.println(p_georeach.ReachabilityQuery(12344377, rect));
			sumtime += System.currentTimeMillis() - start;
		}
		System.out.println(sumtime);*/
		
	}
}
