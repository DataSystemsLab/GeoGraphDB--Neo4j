package def;

import java.util.*;
import java.io.*;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Index implements ReachabilityQuerySolver{
	
	HashSet<Integer> VisitedNodes;
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	public HashSet<Integer> RangeQuery(String layername, Rectangle rect)
	{
		HashSet<Integer> hs = new HashSet();
		
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String range_query = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/findGeometriesInBBox";
		
		WebResource resource = Client.create().resource(range_query);
		String entity = "{ \"layer\": \""+layername+"\", \"minx\": "+rect.min_x+", \"maxx\":"+rect.max_x+", \"miny\": "+rect.min_y+", \"maxy\": "+rect.min_y+" }";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		response.close();
		
		JSONArray arr = JSONArray.fromObject(result);
		for(int i = 0;i<arr.size();i++)
		{
			JSONObject jsonObject = arr.getJSONObject(i);
			result = jsonObject.getString("data");
			
			jsonObject = JSONObject.fromObject(result);
			result = jsonObject.getString("id");
			Integer id = Integer.parseInt(result);
			hs.add(id);
		}
		return hs;
	}
	
	public void ConstructWKTRTree()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String spatial_add_node = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/addGeometryWKTToLayer";
		
		String result = p_neo4j_graph_store.Execute("match (a) where has(a.latitude) return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		//System.out.println(result);
		
		ArrayList<String> l = p_neo4j_graph_store.GetExecuteResultData(result);
		for(int i = 0;i<l.size();i++)
		{
			String record = l.get(i);
			String[] ll = record.split(",");
			int id = Integer.parseInt(ll[0]);
			double latitude = Double.parseDouble(ll[1]);
			double longitude = Double.parseDouble(ll[2]);
			/*System.out.println(id);
			System.out.println(latitude);
			System.out.println(longitude);*/
			
			WebResource resource = Client.create().resource(spatial_add_node);
			String entity = "{ \n \"layer\": \"geom\", \n \"geometry\" : \"POINT(" + latitude + " " + longitude + ") \", \n \"id\": "+ id + " \n} ";
			System.out.println(entity);
			
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
			System.out.println(response.getEntity(String.class));
			response.close();
		}
		
	}
	
	public void CreatePointLayer(String layername)
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String create = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/addSimplePointLayer";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"layer\" : \""+layername+"\", \"lat\" : \"lat\", \"lon\" : \"lon\" }";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	public void CreateSpatialIndex(String layername)
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String create = SERVER_ROOT_URI + "index/node/";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"name\" : \""+layername+"\", \"config\" : { \"provider\" : \"spatial\",\"geometry_type\" : \"point\",\"lat\" : \"lat\", \"lon\" : \"lon\"}}";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	public void CreatePoints()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String spatial_create_node = SERVER_ROOT_URI + "node";
		
		String result = p_neo4j_graph_store.Execute("match (a) where has(a.latitude) return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		//System.out.println(result);
		
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
				/*System.out.println(id);
				System.out.println(latitude);
				System.out.println(longitude);*/
				
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
		String SERVER_ROOT_URI="http://localhost:7474/db/data";
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
	        	String entity = "{\"layer\" : \"simplepointlayer\", \"node\" : \"http://localhost:7474/db/data/node/"+id+"\"}";
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
	
	private void TraversalInEdgeNodes(int start_id)
	{
		ArrayList<Integer> in_neighbors = p_neo4j_graph_store.GetInNeighbors(start_id);
		for(int i = 0;i<in_neighbors.size();i++)
		{
			int in_neighbor = in_neighbors.get(i);
			if(VisitedNodes.add(in_neighbor))
			{
				TraversalInEdgeNodes(in_neighbor);
			}
		}
	}
	
	public void CreateTransitiveClosure()
	{
		ArrayList<Integer> spatial_nodes = p_neo4j_graph_store.GetSpatialVertices();
		for(int i = 0;i<spatial_nodes.size();i++)
		{
			int id = spatial_nodes.get(i);
			System.out.println(id);
			VisitedNodes = new HashSet();
			TraversalInEdgeNodes(id);
			Iterator iter = VisitedNodes.iterator();
			while(iter.hasNext())
			{
				String query = "match (a) where id(a) = "+iter.next()+" set a.reach_nodes = a.reach_nodes + "+id;
				String result = p_neo4j_graph_store.Execute(query);
				System.out.println(result);
			}
		}
	}
	
	void Preprocess
	
	public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		HashSet<Integer> hs = new HashSet();
		
		String reach_nodes = p_neo4j_graph_store.GetVertexAttributeValue(start_id, "reach_nodes");
		reach_nodes = reach_nodes.substring(1, reach_nodes.length()-1);
		String[] l = reach_nodes.split(",");
		for(int i = 0;i<l.length;i++)
		{
			int id = Integer.parseInt(l[i]);
			if(hs.contains(id))
				return true;
		}
		return false;
	}
}