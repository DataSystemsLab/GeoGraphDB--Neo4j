package def;

import java.util.ArrayList;
import java.util.HashSet;

public class Experiment_7_27 {	
	
	private static long graph_size = 3774768;
	private static int experiment_node_count = 500;
	private static double spatial_total_range = 1000;
			
	public static void main(String[] args) {
		
		OwnMethods p_own = new OwnMethods();
		
		Traversal traversal = new Traversal();
		ReachabilityIndex reachindex = new ReachabilityIndex();
		GeoReach georeach = new GeoReach();
		
		String result_file_path = "/home/yuhansun/Documents/Real_data/Patents/query_time.txt";
		boolean break_flag = false;
		
		for(int ratio = 20;ratio<100;ratio+=20)
		{
			p_own.WriteFile(result_file_path, true, "ratio=" + ratio + "\n");
			p_own.WriteFile(result_file_path, true, "rectsize\t"+"traversal_time\t"+"reachindex_time\t"+"get_tran_time\t"+"judge_time\t"+"GeoSpa_time\n");
			
			String graph_label = "Graph_Random_" + ratio;
			String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/" + ratio;
			HashSet<String> hs = p_own.GenerateRandomInteger(graph_size, experiment_node_count);
			ArrayList<String> al = p_own.GenerateStartNode(hs, graph_label);
			p_own.WriteFile(filepath + "/experiment_node.txt", false, al);
					
			for(int j = 1;j<60;j+=10)
			{
				double rect_size = spatial_total_range * Math.sqrt(j/100.0);
				MyRectangle query_rect = new MyRectangle(0, 0, rect_size, rect_size);
				
				long time_traversal = 0,time_reachindex = 0,time_georeach = 0;
				for(int i = 0;i<al.size();i++)
				{
					System.out.println(i);
					int id = Integer.parseInt(al.get(i));
					System.out.println(id);
					
					traversal.VisitedVertices.clear();
					long start = System.currentTimeMillis();
					boolean result1 = traversal.ReachabilityQuery(id, query_rect);
					time_traversal+=System.currentTimeMillis() - start;
					System.out.println(result1);
					
					start = System.currentTimeMillis();
					boolean result2 = reachindex.ReachabilityQuery(id, query_rect, null, graph_label);
					time_reachindex+=System.currentTimeMillis() - start;
					System.out.println(result2);
					
					georeach.VisitedVertices.clear();
					start = System.currentTimeMillis();
					boolean result3 = georeach.ReachabilityQuery(id, query_rect);
					time_reachindex+=System.currentTimeMillis() - start;
					System.out.println(result3);
					
					if(result1!=result2 || result1!=result3)
					{
						System.out.println(id);
						System.out.println(rect_size);
						break_flag=true;
						break;
					}
				}
				if(break_flag)
					break;
				p_own.WriteFile(result_file_path, true, j/100.0+"\t"+time_traversal+"\t"+time_reachindex+"\t"+reachindex.GetTranTime+"\t"+reachindex.JudgeTime+"\t"+time_georeach+"\n");
			}
			if(break_flag)
				break;
			
			p_own.WriteFile(result_file_path, true, "\n");
		}
	}

}
