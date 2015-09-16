package def;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class OwnMethods_Test {

	public static void main(String[] args) {
		
		//GenerateTestData(16384, 500, "Graph_node", "/home/yuhansun/Documents/Synthetic_data/16000_1/test_graph_ids.txt");	
		//GenerateTestData(65536, 500, "Graph_65536_16_1", "/home/yuhansun/Documents/Synthetic_data/65536_16_1/test_graph_ids.txt");
		//GenerateTestData(262144, 500, "Graph_262144_18_1", "/home/yuhansun/Documents/Synthetic_data/262144_18_1/test_graph_ids.txt");
		//GenerateTestData(262144, 1000, "Graph_18_16", "/home/yuhansun/Documents/Synthetic_data/18_16/test_graph_ids.txt");
		//GenerateTestData(262144, 1000, "DAG_18_1", "/home/yuhansun/Documents/Synthetic_data/DAG/18_1/test_graph_ids.txt");
		//GenerateTestData(262144, 1000, "DAG_18_4", "/home/yuhansun/Documents/Synthetic_data/DAG/18_4/test_graph_ids.txt");
//		System.out.println((OwnMethods.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db"))));
		
		System.out.println(OwnMethods.RestartNeo4jClearCache("Patents"));
		
		/*String filename = "/home/yuhansun/Documents/Synthetic_data/test.txt";
		p_ownmethod.WriteFile(filename,false,ids);
		
		ArrayList<String> lines = p_ownmethod.ReadFile(filename);
		System.out.println(lines);*/
//		OwnMethods test = new OwnMethods();
//		File file = new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db");
//		System.out.println(test.getDirSize(file));
//		System.out.println(PostgresJDBC.StartServer());
	}
	
	//generate unique id(a) for experiment
	public static void GenerateTestData(int graph_size, int node_count, String label, String filename)
	{
		OwnMethods p_ownmethod = new OwnMethods();
		HashSet<String> attribute_ids = p_ownmethod.GenerateRandomInteger(graph_size, node_count);
		System.out.println(attribute_ids);
		
		ArrayList<String> graph_ids = p_ownmethod.GenerateStartNode(attribute_ids, label);
		System.out.println(graph_ids);
		
		System.out.printf("%d, %d", attribute_ids.size(), graph_ids.size());
		p_ownmethod.WriteFile(filename, false, graph_ids);
	}

}
