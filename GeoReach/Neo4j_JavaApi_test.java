package GeoReach;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Neo4j_JavaApi_test {

	public static void main(String[] args) 
	{
		//remove those high storage bitmap_128
		/*int node_count = 3774768;
		String dbpath = ("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db");
		int ratio = 80;
		long offset = ratio/20*node_count;
		Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
//		
		try
		{
			for(int id = 0;id<node_count;id++)
			{	
				Transaction tx = p_neo.graphDb.beginTx();
				try
				{
					Node node = p_neo.GetNodeByID(id+offset);
					if(node.hasProperty("ReachGrid_128")&&node.hasProperty("Bitmap_128"))
					{
						int[] reachgrid = (int[]) node.getProperty("ReachGrid_128");
						if(reachgrid.length>204)
							System.out.println(node.removeProperty("Bitmap_128"));
						tx.success();
					}
					
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					tx.close();
				}							
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			p_neo.ShutDown();
		}*/
		
		
//		OwnMethods.ClearCache();
		String str = "Patents";
		String dbpath = ("/home/yuhansun/Documents/Real_data/"+str+"/neo4j-community-2.2.3/data/graph.db");
		Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
		Transaction tx = p_neo.graphDb.beginTx();
		try
		{
			/*System.out.println("start");
			for(int i = 0;i<10;i++)
			{
				Node node = p_neo.GetNodeByID(i);
				System.out.println(node.getId());
			}*/
//			for(int i = 0;i<5;i++)
//			{
//				Random r = new Random();
//				int id = r.nextInt(10000000);
//				long start = System.currentTimeMillis();
//				Node node = p_neo.GetNodeByID(id);
//				long time = System.currentTimeMillis() - start;
//				System.out.println(node.getId());
//				System.out.println(time);
//			}
			int node_count = OwnMethods.GetNodeCount(str);
			for(int i = 0;i<25;i++)
			{
				Node node = p_neo.GetNodeByID(1*node_count+i);
				System.out.println(node.getId());
				
				Iterator<String> iter = node.getPropertyKeys().iterator();
				HashMap<String, String> properties = new HashMap();
				while(iter.hasNext())
				{
					String key = iter.next();
					properties.put(key, node.getProperty(key).toString());
				}
				System.out.println(properties.toString());				
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
