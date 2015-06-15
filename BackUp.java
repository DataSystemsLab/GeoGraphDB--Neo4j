package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class BackUp	{
	
	private String SERVER_ROOT_URI;
	private String longitude_property_name;
	
	Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	public BackUp()
	{
		Config config = new Config();
		SERVER_ROOT_URI = config.GetServerRoot();
		longitude_property_name = config.GetLongitudePropertyName();
	}
	
	public void CreatePoints()
	{
		final String spatial_create_node = SERVER_ROOT_URI + "/node";
		
		String result = p_neo4j_graph_store.Execute("match (a) where has(a." + longitude_property_name + ") return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		
		try {
			PrintWriter writer = new PrintWriter("/home/yuhansun/data/SetLabel/location.txt","UTF-8");
			ArrayList<String> l = p_neo4j_graph_store.GetExecuteResultData(result);
			for(int i = 0;i<l.size();i++)
			{
				String record = l.get(i);
				String[] ll = record.split(",");
				int id = Integer.parseInt(ll[0]);
				double latitude = Double.parseDouble(ll[1]);
				double longitude = Double.parseDouble(ll[2]);
				
				WebResource resource = Client.create().resource(spatial_create_node);
				String entity = "{ \n \"lat\": "+latitude+", \n \"lon\" : "+longitude+", \n \"id\": "+ id + " \n} ";
				
				ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
				result = response.getEntity(String.class);
				JSONObject jsonObject = JSONObject.fromObject(result);
				String location = jsonObject.getString("self");
				response.close();
				writer.write(location+"\n");
			}
			writer.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddPointsToIndex()
	{
		final String spatial_addto_index = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/addNodeToLayer";
		
		ArrayList<String> ids = new ArrayList<String>();
		File file = new File("/home/yuhansun/data/SetLabel/newlocation.txt");
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			
			while ((tempString = reader.readLine()) != null) 
			{
				ids.add(tempString);	                
	        }
	        reader.close();
	        for(int i = 0;i<ids.size();i++)
	        {
	        	String id = ids.get(i);
	        	String entity = "{\"layer\" : \"simplepointlayer\", \"node\" : \""+SERVER_ROOT_URI+"/node/"+id+"\"}";
	        	WebResource resource = Client.create().resource(spatial_addto_index);
	        	ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
	        	String result = response.getEntity(String.class);
	        	System.out.println(result);
	        }
		}
		
		catch (IOException e) 
		{
            e.printStackTrace();
        }
		
		finally 
		{
            if (reader != null) 
            {
                try 
                {
                    reader.close();
                } 
                catch (IOException e1) 
                {
                }
            }
		}
	}
}