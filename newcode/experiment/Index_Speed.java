package experiment;
import java.util.ArrayList;
import java.util.HashSet;

import com.sun.jersey.api.client.WebResource;

import def.*;

public class Index_Speed {

	public static void main(String[] args) 
	{
		String datasource = "Patents";
		long graph_size;
		double spatial_total_range = 1000;
		int ratio = 80;
		String graph_label = "Graph_Random_" + ratio;
		graph_size = OwnMethods.GetNodeCount(datasource);
		String resultpath = "/home/yuhansun/Documents/Real_data/"+datasource+"/index_speed.csv";
		OwnMethods.WriteFile(resultpath, true, "node_count\ttime\n");
		
		int experiment_node_count = 1;
		while(experiment_node_count<=1000)
		{
			System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
			Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store("");
			WebResource source = p_neo.GetCypherResource();
			long time = 0;
			for(int k = 0;k<11;k++)
			{
				
				HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, experiment_node_count);
				long start = System.currentTimeMillis();
				ArrayList<String> al = OwnMethods.GenerateStartNode(source, hs, graph_label);
				if(k == 0)
					continue;
				time += System.currentTimeMillis() - start;
				
			}
			OwnMethods.WriteFile(resultpath, true, experiment_node_count+"\t"+time/10.0+"\n");
			if(experiment_node_count<100)
				experiment_node_count+=10;
			else
				experiment_node_count+=100;
			
			
		}
		OwnMethods.WriteFile(resultpath, true, "\n");	
	}

}
