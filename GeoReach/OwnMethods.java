package GeoReach;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.neo4j.graphdb.Node;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.sun.jersey.api.client.WebResource;

public class OwnMethods {
	
	public static ArrayList<Long> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt";
		int offset = OwnMethods.GetNodeCount(datasource);
		ArrayList<Long> al = new ArrayList<Long>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(Long.parseLong(temp)+offset);
			}
			reader.close();
		}
		catch(Exception e)
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
		return al;
	}
	
	//Print elements in an array
	public static void PrintArray(String[] l)
	{
		for(int i = 0;i<l.length;i++)
			System.out.print(l[i]+"\t");
		System.out.print("\n");
	}
	
	//Generate Random node_count vertices in the range(0, graph_size) which is attribute id
	public static HashSet<Long> GenerateRandomInteger(long graph_size, int node_count)
	{
		HashSet<Long> ids = new HashSet();
		
		Random random = new Random();
		while(ids.size()<node_count)
		{
			Long id = (long) (random.nextDouble()*graph_size);
			ids.add(id);
		}
		
		return ids;
	}
	
	//Generate absolute id in database depends on attribute_id and node label
	public static ArrayList<String> GenerateStartNode(WebResource resource, HashSet<String> attribute_ids, String label)
	{
		String query = "match (a:" + label + ") where a.id in " + attribute_ids.toString() + " return id(a)";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		ArrayList<String> graph_ids = Neo4j_Graph_Store.GetExecuteResultData(result);
		return graph_ids;
	}
	
	//Generate absolute id in database depends on attribute_id and node label
	public static ArrayList<String> GenerateStartNode(HashSet<String> attribute_ids, String label)
	{
		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		String query = "match (a:" + label + ") where a.id in " + attribute_ids.toString() + " return id(a)";
		String result = p_neo4j_graph_store.Execute(query);
		ArrayList<String> graph_ids = Neo4j_Graph_Store.GetExecuteResultData(result);
		return graph_ids;
	}
	
	public ArrayList<String> ReadFile(String filename)
	{
		ArrayList<String> lines = new ArrayList<String>();
		
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
	
	public static void WriteFile(String filename, boolean app, ArrayList<String> lines)
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
	
	public static void WriteFile(String filename, boolean app, String str)
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
	
	public static int GetNodeCount(String datasource)
	{
		int node_count = 0;
		File file = null;
		BufferedReader reader = null;
		try
		{
			file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/graph.txt");
			reader = new BufferedReader(new FileReader(file));
			String str = reader.readLine();
			String[] l = str.split(" ");
			node_count = Integer.parseInt(l[0]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return node_count;		
	}
	
	public static String ClearCache()
	{
		//String[] command = {"/bin/bash","-c","echo data| sudo -S ls"};
		String []cmd = {"/bin/bash","-c","echo data | sudo -S sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String RestartNeo4jClearCache(String datasource)
	{
		String result = "";
		result += Neo4j_Graph_Store.StopMyServer(datasource);
		result += ClearCache();
		result += Neo4j_Graph_Store.StartMyServer(datasource);
		return result;
	}
	
	public static String Serialize_RoarBitmap_ToString(RoaringBitmap r)
	{
		r.runOptimize();
				
		ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
        // If there were runs of consecutive values, you could
        // call mrb.runOptimize(); to improve compression 
        try {
			r.serialize(new DataOutputStream(new OutputStream(){
			    ByteBuffer mBB;
			    OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
			    public void close() {}
			    public void flush() {}
			    public void write(int b) {
			        mBB.put((byte) b);}
			    public void write(byte[] b) {mBB.put(b);}            
			    public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
			}.init(outbb)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //
        outbb.flip();
        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
        return serializedstring;
	}
	
	public static ImmutableRoaringBitmap Deserialize_String_ToRoarBitmap(String serializedstring)
	{
		ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(serializedstring));
	    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(newbb);
	    return ir;
	}
	
	public static void PrintNode(Node node)
	{
		Iterator<String> iter = node.getPropertyKeys().iterator();
		HashMap<String, String> properties = new HashMap();
		while(iter.hasNext())
		{
			String key = iter.next();
			properties.put(key, node.getProperty(key).toString());
		}
		System.out.println(properties.toString());
	}
}
