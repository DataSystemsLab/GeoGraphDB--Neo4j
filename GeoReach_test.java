package def;

import java.util.*;

public class GeoReach_test 
{	
	private static GeoReach p_georeach = new GeoReach();
	
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
		
		Rectangle rect = new Rectangle(0,0,300,300);
		System.out.println(p_georeach.ReachabilityQuery(4022514, rect));
	}
}
