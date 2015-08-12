package def;

import java.util.ArrayList;
import java.util.HashSet;

public class Traversal_test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Traversal traversal = new Traversal();
//		MyRectangle rect = new MyRectangle(0,0,10,10);
//		System.out.println(traversal.ReachabilityQuery(6780075, rect));
		long graph_size = OwnMethods.GetNodeCount("Patents");
		int experiment_node_count = 100;
		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, experiment_node_count);
		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, "Graph_Random_20");
		
		MyRectangle rect = new MyRectangle(0,0,10,10);
		Traversal tra = new Traversal();
		long time = 0;
		for(int i = 0;i<al.size();i++)
		{
			System.out.println(i);
			int id = Integer.parseInt(al.get(i));
			System.out.println(id);
			long start = System.currentTimeMillis();
			boolean result = tra.ReachabilityQuery(id, rect);
			time+= System.currentTimeMillis() - start;
		}
		System.out.println(time);
		
	}

}
