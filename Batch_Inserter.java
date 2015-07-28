package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Batch_Inserter {
	
	public static void CreateUniqueConstraint()
	{
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		p_neo.Execute("create unique constraint on (n:Transitive_Closure) assert n.id is unique");
		for(int i = 0;i<100;i+=20)
		{
			p_neo.Execute("create constraint on (n:Graph_Random_" + Integer.toString(i) + ") assert n.id is unique");
			p_neo.Execute("create constraint on (n:RTree_Random_" + Integer.toString(i) + ") assert n.id is unique");
		}
	}
	
	public static void SetRMBR()
	{
		BatchInserter inserter = null;
		BufferedReader reader = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		long offset = 3774768 * 4;
		
		try
		{
			inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db").getAbsolutePath(),config);
			
			File file = null;
			String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/80";
			
			//nonspatial entities
			file = new File(filepath + "/nonspatial_entity.txt");	

				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					double minx = Double.parseDouble(l[6]);
					if(minx < 0)
						continue;
					double miny = Double.parseDouble(l[7]);
					double maxx = Double.parseDouble(l[8]);
					double maxy = Double.parseDouble(l[9]);
					int id = Integer.parseInt(l[0]);
					inserter.setNodeProperty(id + offset, "RMBR_minx", minx);
					inserter.setNodeProperty(id + offset, "RMBR_miny", miny);
					inserter.setNodeProperty(id + offset, "RMBR_maxx", maxx);
					inserter.setNodeProperty(id + offset, "RMBR_maxy", maxy);
					
					/*Map<String, Object> properties = inserter.getNodeProperties(id + offset);
					properties.put("RMBR_minx", minx);
					properties.put("RMBR_miny", miny);
					properties.put("RMBR_maxx", maxx);
					properties.put("RMBR_maxy", maxy);
					inserter.setNodeProperties(id + offset, properties);*/
				}
				reader.close();
		

			
			//spatial entities
			file = new File(filepath + "/spatial_entity.txt");
				reader = new BufferedReader(new FileReader(file));
				tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					double minx = Double.parseDouble(l[6]);
					if(minx < 0)
						continue;
					double miny = Double.parseDouble(l[7]);
					double maxx = Double.parseDouble(l[8]);
					double maxy = Double.parseDouble(l[9]);
					int id = Integer.parseInt(l[0]);
					
					inserter.setNodeProperty(id + offset, "RMBR_minx", minx);
					inserter.setNodeProperty(id + offset, "RMBR_miny", miny);
					inserter.setNodeProperty(id + offset, "RMBR_maxx", maxx);
					inserter.setNodeProperty(id + offset, "RMBR_maxy", maxy);
					/*Map<String, Object> properties = inserter.getNodeProperties(id + offset);
					properties.put("RMBR_minx", minx);
					properties.put("RMBR_miny", miny);
					properties.put("RMBR_maxx", maxx);
					properties.put("RMBR_maxy", maxy);
					inserter.setNodeProperties(id + offset, properties);*/
				}
				reader.close();
			

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
	

	public static void main(String[] args) {
		
		//CreateUniqueConstraint();
		//LoadRTreeNodes();
		//SetRMBR();
		// TODO Auto-generated method stub
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "16g");
		Label transitive_node_label = DynamicLabel.label("Transitive_Closure");
		RelationshipType transitive_relationship_lebal = DynamicRelationshipType.withName("REACH");
		String db_path = "/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db";
		
		try
		{
			OwnMethods p_own = new OwnMethods();
			long size = p_own.getDirSize(new File(db_path));
			//transitive closure nodes and relationships
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			for(int i = 0;i<3774768;i++)
			{
				Map<String, Object> properties = new HashMap();
				properties.put("id", i);
				inserter.createNode(i, properties, transitive_node_label);
			}
			
			
			file = new File("/home/yuhansun/Documents/Real_data/Patents/transitive_closure.txt");

			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null)
			{
				tempString = tempString.trim();
				String[] l = tempString.split(" ");
				long start = Long.parseLong(l[0]);
				long count = Long.parseLong(l[1]);
				if(count == 0)
					continue;
				for(int i = 2;i<l.length;i++)
				{
					long end = Long.parseLong(l[i]);
					inserter.createRelationship(start, end, transitive_relationship_lebal, null);
				}
			}
			reader.close();
			size = p_own.getDirSize(new File(db_path)) - size;
			p_own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/size.txt", true, ""+size);
		}
		
		/*try
		{
//			inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db").getAbsolutePath(), config);
			inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/test/neo4j-community-2.2.3/data/test.db").getAbsolutePath(), config);
			
			long offset = 3774768 * 4;
			Label graph_label = DynamicLabel.label("Graph_Random_20");
			RelationshipType graph_rel = DynamicRelationshipType.withName("LINK");
			String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/20";
			BufferedReader reader = null;
			File file = null;
			
			//nonspatial entities
			file = new File(filepath + "/nonspatial_entity.txt");	
			try
			{
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					Map<String, Object> properties = new HashMap();
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					properties.put("id", id);
					//inserter.createNode(id + offset, properties, graph_label);
					inserter.createNode(properties, graph_label);
					
				}
				reader.close();
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
			
			//spatial entities
			file = new File(filepath + "/spatial_entity.txt");
			try
			{
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					Map<String, Object> properties = new HashMap();
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					double lon = Double.parseDouble(l[2]);
					double lat = Double.parseDouble(l[3]);
					properties.put("id", id);
					properties.put("longitude", lon);
					properties.put("latitude", lat);
//					inserter.createNode(id + offset, properties, graph_label);
					inserter.createNode(properties, graph_label);
					
				}
				reader.close();
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
			}*/
			
//			//graph relationships
//			file = new File("/home/yuhansun/Documents/Real_data/Patents/graph_relationships.txt");			
//			try
//			{				
//				reader = new BufferedReader(new FileReader(file));
//				String tempString = null;
//				while((tempString = reader.readLine())!=null)
//				{				
//					String[] l = tempString.split(" ");
//					long start = Long.parseLong(l[0]);
//					long end = Long.parseLong(l[1]);
//					//System.out.println(start);
//					inserter.createRelationship(start + offset, end + offset, graph_rel, null);
//				}
//				reader.close();
//			}
//			catch(IOException e)
//			{
//				e.printStackTrace();
//			}
//			finally
//			{
//				if(reader!=null)
//				{
//					try
//					{
//						reader.close();
//					}
//					catch(IOException e)
//					{					 
//					}
//				}
//			}
//		}
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

}
