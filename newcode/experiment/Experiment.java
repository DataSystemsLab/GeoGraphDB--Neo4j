package experiment;
import def.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Experiment {

	public static void main(String[] args) 
	{		
//		CheckRMBRLocation();
		
//		CheckBitmap();
		
		long start = System.currentTimeMillis();
		SetNull();
		System.out.println(System.currentTimeMillis() - start);
		
		/*for(int  i= 0;i<datasource_a.size();i++)
		{
			String str = datasource_a.get(i);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
//				System.out.println("create index "+str+"_random_"+ratio+"_clustered_gist on "+str+"_random_"+ratio+"_clustered using gist(location);");
				String relname = str+"_random_"+ratio+"_clustered_gist";
				System.out.print(("select pg_relation_size('"+relname+"') as "+relname+";"));
			}
		}
		for(int  i= 0;i<datasource_a.size();i++)
		{
			String str = datasource_a.get(i);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
//				System.out.println("create index "+str+"_random_"+ratio+"_zipf_gist on "+str+"_random_"+ratio+"_zipf using gist(location);");
				String relname = str+"_random_"+ratio+"_zipf_gist";
				System.out.print(("select pg_relation_size('"+relname+"') as "+relname+";"));
			}
		}*/
		/*Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		
		GeoReach georeach = new GeoReach();
		Index index = new Index();
		Traversal traversal = new Traversal();
		
		MyRectangle query_rect = new MyRectangle(0, 0, 500, 500);

		OwnMethods p_ownmethods = new OwnMethods();
		String root = "/home/yuhansun/Documents/Synthetic_data";
		
		//String filename = root + "/16000_1/test_graph_ids.txt";
		//String filename = root + "/65536_16_1/test_graph_ids.txt";
		//String filename = root + "/262144_18_1/test_graph_ids.txt";
		String filename = root + "/DAG/18_4/test_graph_ids.txt";
		
		
		ArrayList<String> graph_ids = p_ownmethods.ReadFile(filename);
		
		
		long time1 = 0,time2 = 0,time3 = 0;
		
		for(int i = 0;i<500;i++)
		{
			System.out.println(i);
			int id = Integer.parseInt(graph_ids.get(i));
			System.out.println(id);

			traversal.VisitedVertices.clear();
			long start = System.currentTimeMillis();
			boolean result1 = traversal.ReachabilityQuery(id, query_rect);
			time1+=System.currentTimeMillis() - start;
			System.out.println(result1);
			
			start = System.currentTimeMillis();
			//boolean result2 = index.ReachabilityQuery(id, query_rect,"RTree_262144_18_1","Transitive_closure_262144_18_1");
			//boolean result2 = index.ReachabilityQuery(id, query_rect);
			//boolean result2 = index.ReachabilityQuery(id, query_rect,"DAGRTree_18_1","DAGTransitive_closure_18_1");
			
			time2+=System.currentTimeMillis() - start;
			//System.out.println(result2);
			
			georeach.VisitedVertices.clear();
			start = System.currentTimeMillis();
			boolean result3 = georeach.ReachabilityQuery(id, query_rect);
			time3+=System.currentTimeMillis() - start;
			System.out.println(result3);
						
			//if(result1!=result2 || result1!=result3)
			if(result1!=result3)
			{
				System.out.println(id);
				break;
			}
		}
		
		System.out.printf("%s, %s, %s\n", time1, time2, time3);
		System.out.printf("%s, %s, %s\n", index.GetTranTime, index.GetRTreeTime, index.JudgeTime);
		System.out.printf("%s, %s",index.QueryTime, index.BuildListTime);*/
	}
	
	public static void SetNull()
	{
		String property_suffix = "_clustered";
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
		
		for(int  i= 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
			int ratio = 20;
			int node_count = OwnMethods.GetNodeCount(datasource);
			File file = null;
			BufferedReader reader = null;
			long offset = ratio/20*node_count;
			String dbpath = ("/home/yuhansun/Documents/Real_data/"+datasource+"/neo4j-community-2.2.3/data/graph.db");
			Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
			Transaction tx = null;
			try
			{
				int bulksize = 1000;
				long id = offset;
				while(id<5*node_count - bulksize)
				{
					try
					{
						tx = p_neo.graphDb.beginTx();
						for(long j = id;j<id+bulksize;j++)
						{
							Node node = p_neo.GetNodeByID(j);
							node.removeProperty("longitude"+property_suffix);
							node.removeProperty("latitude"+property_suffix);
							node.removeProperty("RMBR_minx"+property_suffix);
							node.removeProperty("RMBR_miny"+property_suffix);
							node.removeProperty("RMBR_maxx"+property_suffix);
							node.removeProperty("RMBR_maxy"+property_suffix);
						}
						tx.success();
						id+=bulksize;
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
				tx = p_neo.graphDb.beginTx();
				for(long j = id;j<5*node_count;j++)
				{
					Node node = p_neo.GetNodeByID(j);
					node.removeProperty("longitude"+property_suffix);
					node.removeProperty("latitude"+property_suffix);
					node.removeProperty("RMBR_minx"+property_suffix);
					node.removeProperty("RMBR_miny"+property_suffix);
					node.removeProperty("RMBR_maxx"+property_suffix);
					node.removeProperty("RMBR_maxy"+property_suffix);
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
	
	public static void CheckBitmap()
	{
		String property_suffix = "_clustered";
		String file_suffix = "Clustered_distributed";
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
		
		for(int  i= 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
			int ratio = 80;
			int node_count = OwnMethods.GetNodeCount(datasource);
			File file = null;
			BufferedReader reader = null;
			long offset = ratio/20*node_count;
			String dbpath = ("/home/yuhansun/Documents/Real_data/"+datasource+"/neo4j-community-2.2.3/data/graph.db");
			Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
			Transaction tx = p_neo.graphDb.beginTx();
			try
			{
				file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/GeoReachGrid_128/"+file_suffix+"/"+"Bitmap_"+ratio+".txt");
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String str = null;
				int false_count = 0;
				while((str = reader.readLine())!=null)
				{
					String[] l = str.split("\t");
					int id = Integer.parseInt(l[0]);
					Node node = p_neo.GetNodeByID(id+offset);
					if(node.hasProperty("Bitmap_128"+property_suffix))
					{
						String bitmap = node.getProperty("Bitmap_128"+property_suffix).toString();
						if(bitmap.equals(l[1]))
							continue;
						else
						{
							System.out.println(datasource+"\t");
							OwnMethods.PrintNode(node);
							false_count++;
							if(false_count == 100)
								break;
						}
					}
					else
					{
						System.out.println(datasource+"\t");
						OwnMethods.PrintNode(node);
						false_count++;
						if(false_count == 100)
							break;
					}
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
	
	public static void CheckRMBRLocation()
	{
		String property_suffix = "_clustered";
		String file_suffix = "Clustered_distributed";
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
		
		for(int  i= 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
			int ratio = 40;
			int node_count = OwnMethods.GetNodeCount(datasource);
			File file = null;
			BufferedReader reader = null;
			long offset = ratio/20*node_count;
			String dbpath = ("/home/yuhansun/Documents/Real_data/"+datasource+"/neo4j-community-2.2.3/data/graph.db");
			Neo4j_JavaApi p_neo = new Neo4j_JavaApi(dbpath);
			Transaction tx = p_neo.graphDb.beginTx();
			try
			{
				file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/"+file_suffix+"/"+ratio+"/entity.txt");
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String str = null;
				int false_count = 0;
				while((str = reader.readLine())!=null)
				{
					String[] l = str.split(" ");
					int id = Integer.parseInt(l[0]);
					Node node = p_neo.GetNodeByID(id+offset);
					if(Integer.parseInt(l[1]) == 1)
					{
						if(!node.hasProperty("longitude"+property_suffix))
						{
							System.out.println(datasource+"\t");
							OwnMethods.PrintNode(node);
							false_count++;
							if(false_count == 100)
								break;
						}
						else
						{
							double lon = (Double) node.getProperty("longitude"+property_suffix);
							double lat = (Double) node.getProperty("latitude"+property_suffix);
							double lon_read = Double.parseDouble(l[2]);
							double lat_read = Double.parseDouble(l[3]);
							if(Math.abs(lon - lon_read)>0.0000001||Math.abs(lat - lat_read)>0.0000001)
							{
								System.out.println(datasource+"\t");
								OwnMethods.PrintNode(node);
								false_count++;
								if(false_count == 100)
									break;							}						
						}
					}
					else
					{
						if(node.hasProperty("longitude"+property_suffix))
						{
							System.out.println(datasource+"\t");
							OwnMethods.PrintNode(node);
							false_count++;
							if(false_count == 100)
								break;						}
					}
					
					double minx_read  = Double.parseDouble(l[6]);
					if(minx_read>=0)
					{
						double miny_read = Double.parseDouble(l[7]);
						double maxx_read = Double.parseDouble(l[8]);
						double maxy_read = Double.parseDouble(l[9]);
						
						
						if(node.hasProperty("RMBR_minx"+property_suffix))
						{
							double minx = Double.parseDouble(node.getProperty("RMBR_minx"+property_suffix).toString());
							double miny = Double.parseDouble(node.getProperty("RMBR_miny"+property_suffix).toString());
							double maxx = Double.parseDouble(node.getProperty("RMBR_maxx"+property_suffix).toString());
							double maxy = Double.parseDouble(node.getProperty("RMBR_maxy"+property_suffix).toString());
							
							if(Math.abs(minx - minx_read)>0.0000001||Math.abs(miny - miny_read)>0.0000001||Math.abs(maxx - maxx_read)>0.0000001||Math.abs(maxy - maxy_read)>0.0000001)
							{
								System.out.println(datasource+"\t");
								OwnMethods.PrintNode(node);
								false_count++;
								if(false_count == 100)
									break;							}
						}
						else
						{
							System.out.println(datasource+"\t");
							OwnMethods.PrintNode(node);
							false_count++;
							if(false_count == 100)
								break;						}
					}
					else
					{
						if(node.hasProperty("RMBR_minx"+property_suffix))
						{
							System.out.println(datasource+"\t");
							OwnMethods.PrintNode(node);
							false_count++;
							if(false_count == 100)
								break;						}
					}
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
}
