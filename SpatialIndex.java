package def;

import java.io.File;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class SpatialIndex implements ReachabilityQuerySolver{
	
	Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
	Index p_index = new Index();
	OwnMethods p_own = new OwnMethods();
	
	public void Construct_RTree_Index()
	{
		for(int ratio = 20;ratio<100;ratio+=20)
		{
			long database_size = p_own.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db"));
			
			String label = "RTree_Random_" + ratio;
			String tree_name = "RTree_Random_" + ratio;
			
			p_index.CreatePointLayer(tree_name);
			
			String query = "match (a:"+label+") return id(a)";
			String result = p_neo.Execute(query);
			JsonArray jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			for(int j = 0;j<jsonArr.size();j++)
			{
				JsonObject jsonOb = (JsonObject) jsonArr.get(j);
				JsonArray arr = jsonOb.get("row").getAsJsonArray();
				long id = arr.get(0).getAsLong();
				p_index.AddOneNodeToPointLayer(tree_name, id);
			}
			long rtree_size = p_own.getDirSize(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db")) - database_size;
			System.out.println(tree_name + ": " + rtree_size);
		}
	}

	public void Preprocess() {
		// TODO Auto-generated method stub
		Construct_RTree_Index();
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) {
		// TODO Auto-generated method stub
		return false;
	}
	
	

}
