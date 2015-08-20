package def;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class GeoReach_Integrated_test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String datasource = "Patents";
		int graph_size = OwnMethods.GetNodeCount(datasource);
		double experiment_node_count = 10;
		String graph_label = "Graph_Random_20";
		double spatial_total_range = 1000;
		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
		OwnMethods.GetNodeCount(datasource);
		
		Random r = new Random();
		HashMap<Integer, ArrayList<Double>> hs_x = new HashMap();
		HashMap<Integer, ArrayList<Double>> hs_y = new HashMap();
		for(int j = 1;j<60;j++)
		{
			ArrayList<Double> lx = new ArrayList();
			ArrayList<Double> ly = new ArrayList();
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
		for(int j = 1;j<60;j+=10)
		{
			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
			ArrayList<Double> lx = hs_x.get(j);
			ArrayList<Double> ly = hs_x.get(j);
			
			for(int i = 0;i<al.size();i++)
			{
				int id = Integer.parseInt(al.get(i));
				double x = lx.get(i);
				double y = ly.get(i);
				MyRectangle query_rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
				
				Traversal tra = new Traversal();
				MyRectangle rect = new MyRectangle(0,0,spatial_total_range,spatial_total_range);
				GeoReach_Integrate inte = new GeoReach_Integrate(rect, 5);
				boolean result1 = tra.ReachabilityQuery(id, query_rect);
				boolean result2 = inte.ReachabilityQuery(id, query_rect);
				
				System.out.println(id);
				System.out.println(result1);
				System.out.println(result2);
				
				if(result1!=result2)
				{
					System.out.println(x);
					System.out.println(y);
					System.out.println(rect_size);
					
					isbreak = true;
					break;
				}
			}
			if(isbreak)
				break;
		}
		
	}

}
