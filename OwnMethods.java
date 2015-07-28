package def;

import java.io.*;
import java.util.*;

public class OwnMethods {
	
	public HashSet<String> GenerateRandomInteger(long graph_size, int node_count)
	{
		HashSet<String> ids = new HashSet();
		
		Random random = new Random();
		while(ids.size()<node_count)
		{
			Integer id = (int) (random.nextDouble()*graph_size);
			ids.add(id.toString());
		}
		
		return ids;
	}
	
	public ArrayList<String> GenerateStartNode(HashSet<String> attribute_ids, String label)
	{
		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		String query = "match (a:" + label + ") where a.id in " + attribute_ids.toString() + " return id(a)";
		String result = p_neo4j_graph_store.Execute(query);
		ArrayList<String> graph_ids = p_neo4j_graph_store.GetExecuteResultData(result);
		return graph_ids;
	}
	
	public ArrayList<String> ReadFile(String filename)
	{
		ArrayList<String> lines = new ArrayList();
		
		File file = new File(filename);
		BufferedReader reader = null;
		
		try
		{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null)
			{
				lines.add(tempString);
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
		return lines;
	}
	
	public void WriteFile(String filename, boolean app, ArrayList<String> lines)
	{
		try 
		{
			FileWriter fw = new FileWriter(filename,app);
			for(int i = 0;i<lines.size();i++)
			{
				fw.write(lines.get(i)+"\n");
			}
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void WriteFile(String filename, boolean app, String str)
	{
		try 
		{
			FileWriter fw = new FileWriter(filename,app);
			fw.write(str);
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}

	public static long getDirSize(File file) {     
        if (file.exists()) {     
            if (file.isDirectory()) {     
                File[] children = file.listFiles();     
                long size = 0;     
                for (File f : children)     
                    size += getDirSize(f);     
                return size;     
            } else {
            	long size = file.length(); 
                return size;     
            }     
        } else {     
            System.out.println("File not exists!");     
            return 0;     
        }     
    }
}
