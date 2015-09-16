package def;

import java.util.Random;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Neo4j_JavaApi_test {

	public static void main(String[] args) 
	{
		OwnMethods.ClearCache();
		String dbpath = ("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db");
		Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
		Transaction tx = p_neo.graphDb.beginTx();
		try
		{
			for(int i = 0;i<5;i++)
			{
				Random r = new Random();
				int id = r.nextInt(10000000);
				long start = System.currentTimeMillis();
				Node node = p_neo.GetNodeByID(id);
				long time = System.currentTimeMillis() - start;
				System.out.println(node.getId());
				System.out.println(time);
			}
			
			tx.success();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			tx.close();
			p_neo.ShutDown();
		}	
		
	}

}
