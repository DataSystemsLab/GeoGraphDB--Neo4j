package def;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

public class Spatial_Reach_Index_test {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		MyRectangle rect = new MyRectangle(0,0,100,100);
		Spatial_Reach_Index spareach = new Spatial_Reach_Index("Patents_Random_20");
		int graph_size = OwnMethods.GetNodeCount("Patents");
		int experiment_node_count = 20;
		String graph_label = "Graph_Random_20";
		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, experiment_node_count);
		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
		System.out.println(al);
		long time = 0;
		for(int i = 0;i<al.size();i++)
		{
			int start_id = Integer.parseInt(al.get(i));
			System.out.println(start_id);
			long start = System.currentTimeMillis();
			boolean result = spareach.ReachabilityQuery(start_id, rect);
			time += System.currentTimeMillis() - start;
			System.out.println(result);
		}
		System.out.println(time);
		spareach.Disconnect();
	}

}
