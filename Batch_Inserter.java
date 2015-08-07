package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
		config.put("dbms.pagecache.memory", "5g");
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

	public static void main(String[] args) 
	{		
		//CreateUniqueConstraint();
		//LoadRTreeNodes();
		//SetRMBR();
		// TODO Auto-generated method stub
		BatchInserter inserter = null;
		BufferedReader reader = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		Label transitive_node_label = DynamicLabel.label("Transitive_Closure");
		RelationshipType transitive_relationship_lebal = DynamicRelationshipType.withName("REACH");
		String db_path = "/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db";
		
		//ReachabilityIndex insert
//		try
//		{
//			Map<Integer,Integer> table = new HashMap();
//			file = new File("/home/yuhansun/Documents/Real_data/Patents/table.txt");
//			reader = new BufferedReader(new FileReader(file));
//			String tempString = null;
//			while((tempString = reader.readLine())!=null)
//			{		
//				String[] l = tempString.split("\t");
//				int first = Integer.parseInt(l[0]);
//				int second = Integer.parseInt(l[1]);
//				table.put(second, first);
//			}
//			reader.close();
//			
//			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
//			File file_reachFrom = new File("/home/yuhansun/Documents/Real_data/Patents/reachFromIndex.txt");
//			File file_reachTo = new File("/home/yuhansun/Documents/Real_data/Patents/reachToIndex.txt");
//			BufferedReader reader_reachFrom = new BufferedReader(new FileReader(file_reachFrom));
//			BufferedReader reader_reachTo = new BufferedReader(new FileReader(file_reachTo));
//			String str_reachFrom = null, str_reachTo = null;
//			Label Reach_Index_Label = DynamicLabel.label("Reachability_Index");
//			while(((str_reachFrom = reader_reachFrom.readLine())!=null)&&((str_reachTo = reader_reachTo.readLine())!=null))
//			{
//				String[] l_rF = str_reachFrom.split("\t");
//				String[] l_rT = str_reachTo.split("\t");
//				int scc_id = Integer.parseInt(l_rF[0]);
//				int id = table.get(scc_id);
//				Map<String, Object> properties = new HashMap();
//				properties.put("scc_id", scc_id);
//				properties.put("id", id);
//				
//				if(l_rF.length>1)
//				{
//					int[] l = new int[l_rF.length-1];
//					for(int i = 0;i<l.length;i++)
//						l[i] = Integer.parseInt(l_rF[i+1]);
//					properties.put("reachFrom", l);
//				}
//				if(l_rT.length>1)
//				{
//					int[] l = new int[l_rT.length-1];
//					for(int i = 0;i<l.length;i++)
//						l[i] = Integer.parseInt(l_rT[i+1]);
//					properties.put("reachTo", l);
//				}
//				inserter.createNode(scc_id, properties, Reach_Index_Label);
//			}
//		}
//		catch(IOException e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//			if(inserter!=null)
//				inserter.shutdown();
//			if(reader!=null)
//			{
//				try
//				{
//					reader.close();
//				}
//				catch(IOException e)
//				{					
//				}
//			}
//		}
		
//		try
//		{
//			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
//			
//			//read convTable and create node
//			Label Reach_Index_Label = DynamicLabel.label("Reachability_Index");
//			file = new File("/home/yuhansun/Documents/Real_data/Patents/table.txt");
//			reader = new BufferedReader(new FileReader(file));
//			String tempString = null;
//			int i = 0;
//			while((tempString = reader.readLine())!=null)
//			{
//				String[] l = tempString.split("\t");
//				int scc_id = Integer.parseInt(l[1]);
//				Map<String, Object> properties = new HashMap();
//				properties.put("id", i);
//				properties.put("scc_id", scc_id);
//				inserter.createNode(scc_id, properties, Reach_Index_Label);
//			}
//			reader.close();
//		}
//		catch(IOException e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//			if(inserter!=null)
//				inserter.shutdown();
//			if(reader!=null)
//			{
//				try
//				{
//					reader.close();
//				}
//				catch(IOException e)
//				{					
//				}
//			}
//		}
//		try
//		{
//			//read index and create relationship
//			file = new File("/home/yuhansun/Documents/Real_data/Patents/reachFromIndex.txt");
//			RelationshipType rel_type = DynamicRelationshipType.withName("REACH_FROM");
//			reader = new BufferedReader(new FileReader(file));
//			String tempString = null;
//			while((tempString = reader.readLine())!=null)
//			{
//				String[] l = tempString.split("\t");
//				if(l.length == 1)
//					continue;
//				int target = Integer.parseInt(l[0]);
//				for(int j = 1;j<l.length;j++)
//				{
//					int source = Integer.parseInt(l[j]);
//					inserter.createRelationship(source, target, rel_type, null);
//				}				
//			}
//			reader.close();
//		}
//		catch(IOException e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//			if(inserter!=null)
//				inserter.shutdown();
//			if(reader!=null)
//			{
//				try
//				{
//					reader.close();
//				}
//				catch(IOException e)
//				{					
//				}
//			}
//		}
//		try
//		{
//			file = new File("/home/yuhansun/Documents/Real_data/Patents/reachToIndex.txt");
//			RelationshipType rel_type = DynamicRelationshipType.withName("REACH_TO");
//			reader = new BufferedReader(new FileReader(file));
//			String tempString = null;
//			while((tempString = reader.readLine())!=null)
//			{
//				String[] l = tempString.split("\t");
//				if(l.length == 1)
//					continue;
//				int source = Integer.parseInt(l[0]);
//				for(int j = 1;j<l.length;j++)
//				{
//					int target = Integer.parseInt(l[j]);
//					inserter.createRelationship(source, target, rel_type, null);
//				}				
//			}
//			reader.close();
//		}
//		catch(IOException e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//			if(inserter!=null)
//				inserter.shutdown();
//			if(reader!=null)
//			{
//				try
//				{
//					reader.close();
//				}
//				catch(IOException e)
//				{					
//				}
//			}
//		}
		
		for(int ratio = 20;ratio<100;ratio+=20)
		{
			
			OwnMethods p_own = new OwnMethods();
			long size = p_own.getDirSize(new File(db_path));
			int offset = ratio / 20 * 3774768;
//			try
//			{
				//transitive closure nodes and relationships
//				inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
//				for(int i = 0;i<3774768;i++)
//				{
//					Map<String, Object> properties = new HashMap();
//					properties.put("id", i);
//					inserter.createNode(i, properties, transitive_node_label);
//				}
//				
//				
//				file = new File("/home/yuhansun/Documents/Real_data/Patents/transitive_closure.txt");
//
//				reader = new BufferedReader(new FileReader(file));
//				String tempString = null;
//				while((tempString = reader.readLine())!=null)
//				{
//					tempString = tempString.trim();
//					String[] l = tempString.split(" ");
//					long start = Long.parseLong(l[0]);
//					long count = Long.parseLong(l[1]);
//					if(count == 0)
//						continue;
//					for(int i = 2;i<l.length;i++)
//					{
//						long end = Long.parseLong(l[i]);
//						inserter.createRelationship(start, end, transitive_relationship_lebal, null);
//					}
//				}
//				reader.close();
//				size = p_own.getDirSize(new File(db_path)) - size;
//				p_own.WriteFile("/home/yuhansun/Documents/Real_data/Patents/size.txt", true, ""+size);
//			}
			
			try
			{
				inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/Patents/neo4j-community-2.2.3/data/graph.db").getAbsolutePath(), config);
				//inserter = BatchInserters.inserter(new File("/home/yuhansun/Documents/Real_data/test/neo4j-community-2.2.3/data/test.db").getAbsolutePath(), config);
				
				Label graph_label = DynamicLabel.label("Graph_Random_" + ratio);
				RelationshipType graph_rel = DynamicRelationshipType.withName("LINK");
				String filepath = "/home/yuhansun/Documents/Real_data/Patents/Random_spatial_distributed/" + ratio;
				reader = null;
				file = null;
				
				//nonspatial entities
				file = new File(filepath + "/nonspatial_entity.txt");	
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				while((tempString = reader.readLine())!=null)
				{
					Map<String, Object> properties = new HashMap();
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					properties.put("id", id);
					
					double minx = Double.parseDouble(l[6]);
					if(minx > 0)
					{
						double miny = Double.parseDouble(l[7]);
						double maxx = Double.parseDouble(l[8]);
						double maxy = Double.parseDouble(l[9]);
						properties.put("RMBR_minx", minx);
						properties.put("RMBR_miny", miny);
						properties.put("RMBR_maxx", maxx);
						properties.put("RMBR_maxy", maxy);
					}						
					inserter.createNode(id + offset, properties, graph_label);									
				}
				reader.close();

				
				//spatial entities
				file = new File(filepath + "/spatial_entity.txt");

				reader = new BufferedReader(new FileReader(file));
				tempString = null;
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
					
					double minx = Double.parseDouble(l[6]);
					if(minx > 0)
					{
						double miny = Double.parseDouble(l[7]);
						double maxx = Double.parseDouble(l[8]);
						double maxy = Double.parseDouble(l[9]);
						properties.put("RMBR_minx", minx);
						properties.put("RMBR_miny", miny);
						properties.put("RMBR_maxx", maxx);
						properties.put("RMBR_maxy", maxy);
					}
					
					inserter.createNode(id + offset, properties, graph_label);					
				}
				reader.close();

				//graph relationships
				file = new File("/home/yuhansun/Documents/Real_data/Patents/graph_relationships.txt");											
				reader = new BufferedReader(new FileReader(file));
				tempString = null;
				while((tempString = reader.readLine())!=null)
				{				
					String[] l = tempString.split(" ");
					long start = Long.parseLong(l[0]);
					long end = Long.parseLong(l[1]);
					//System.out.println(start);
					inserter.createRelationship(start + offset, end + offset, graph_rel, null);
				}
				reader.close();
			}
				
			catch(IOException e)
			{
				e.printStackTrace();
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e1)
					{					
					}
				}
				if(inserter!=null)
					inserter.shutdown();
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

}
