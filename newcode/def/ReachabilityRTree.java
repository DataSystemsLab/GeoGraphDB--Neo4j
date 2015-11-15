package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ReachabilityRTree implements ReachabilityQuerySolver{
	
	private String longitude_property_name;
	private String latitude_property_name;
	private Neo4j_Graph_Store p_neo;
	private Index index;
	
	public ReachabilityRTree(String p_suffix)
	{
		Config config = new Config(p_suffix);
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		p_neo = new Neo4j_Graph_Store(p_suffix);
		index = new Index(p_suffix);
	}
	
	public void LoadRTreeNodes()
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		
		try
		{
			//inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db").getAbsolutePath(),config);
			inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/test/neo4j-community-2.2.3/data/test.db").getAbsolutePath(),config);
			File file = null;
			
			//for(int i = 20;i<100;i+=20)
			int i = 20;
			{
				String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/" + Integer.toString(i);
				Label RTree_label = DynamicLabel.label("RTree_Random_"+Integer.toString(i));

				file = new File(filepath + "/spatial_entity.txt");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				System.out.println(tempString);
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					double lon = Double.parseDouble(l[2]);
					double lat = Double.parseDouble(l[3]);					
					int id = Integer.parseInt(l[0]);
					
					Map<String, Object> properties = new HashMap();
					properties.put("id", id);
					properties.put("lon", lon);
					properties.put("lat", lat);
					
					inserter.createNode(properties, RTree_label);
				}
				reader.close();
			}

		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(inserter!=null)
				inserter.shutdown();
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
	}
	
	public void CreateRTreeIndex()
	{
		BufferedReader reader = null;
		File file = null;
		try
		{
			int i = 20;
			{
				String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/" + Integer.toString(i);
				file = new File(filepath + "/spatial_transitive_closure.txt");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				for(int j = 0;j<383;j++)
				{
					reader.readLine();
				}
				while((tempString = reader.readLine())!=null)
				{
					int index = tempString.indexOf(' ');
					int start = Integer.parseInt(tempString.substring(0, index));
					System.out.println(start);
					tempString = tempString.substring(index + 1, tempString.length());
					index = tempString.indexOf(' ');
					int count = Integer.parseInt(tempString.substring(0, index));
					if(count == 0)
						continue;
					else
					{
						String pointlayername = "Graph_Random_"+Integer.toString(i)+"_"+Integer.toString(start);
						this.index.CreatePointLayer(pointlayername);
						
						tempString.trim();
						tempString = tempString.substring(index + 1, tempString.length());
						String[] l = tempString.split(" ");
						int bulkcount = l.length / 1000;
						for(int j = 0;j<bulkcount;j++)
						{
							String ls = "[" + l[j * 1000];
							for(int k = 1;k<100;k++)
							{
								ls = ls + "," + l[k]; 
							}
							ls+="]";
							
							String query = "match (a:RTree_Random_" + Integer.toString(i) + ") where a.id in " + ls + " return id(a)";
							String result = this.p_neo.Execute(query);
							HashSet<Integer> hs = this.p_neo.GetExecuteResultDataInSet(result);
							if(hs == null)
								System.out.println(start);
							Iterator<Integer> iter = hs.iterator();
							while(iter.hasNext())
								this.index.AddOneNodeToPointLayer(pointlayername, iter.next());
						}
						String ls = "[" + l[bulkcount*1000];
						for(int j = bulkcount*1000 + 1;j<l.length;j++)
						{
							ls = ls + "," + l[j];
						}
						ls+="]";
						String query = "match (a:RTree_Random_" + Integer.toString(i) + ") where a.id in " + ls + " return id(a)"; 
						String result = this.p_neo.Execute(query);
						HashSet<Integer> hs = this.p_neo.GetExecuteResultDataInSet(result);
						if(hs == null)
							System.out.println(start);
						Iterator<Integer> iter = hs.iterator();
						while(iter.hasNext())
							this.index.AddOneNodeToPointLayer(pointlayername, iter.next());								
					}
				}
				reader.close();
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
	}

	public void Preprocess() {
		// TODO Auto-generated method stub
		
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect) {
		// TODO Auto-generated method stub
		return false;
	}
}
