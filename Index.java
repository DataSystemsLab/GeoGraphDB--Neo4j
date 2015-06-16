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
	Queue<Integer> queue;
	
	private String SERVER_ROOT_URI;
	private String longitude_property_name;
	
	Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	public Index()
	{
		Config config = new Config();
		SERVER_ROOT_URI = config.GetServerRoot();
		longitude_property_name = config.GetLongitudePropertyName();
	}
	
	public HashSet<Integer> RangeQuery(String layername, Rectangle rect)
	{
		HashSet<Integer> hs = new HashSet();
		
		final String range_query = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/findGeometriesInBBox";
		
		WebResource resource = Client.create().resource(range_query);
		String entity = "{ \"layer\": \""+layername+"\", \"minx\": "+rect.min_x+", \"maxx\":"+rect.max_x+", \"miny\": "+rect.min_y+", \"maxy\": "+rect.max_y+" }";
		//System.out.println(entity);
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		int status = response.getStatus();
		response.close();
		//System.out.println(result);
		//System.out.println(status);
		
		JSONArray arr = JSONArray.fromObject(result);
		for(int i = 0;i<arr.size();i++)
		{
			JSONObject jsonObject = arr.getJSONObject(i);
			result = jsonObject.getString("data");
			//System.out.println(result);
			
			jsonObject = JSONObject.fromObject(result);
			result = jsonObject.getString("id");
			Integer id = Integer.parseInt(result);
			hs.add(id);
		}
		return hs;
	}
	
	public void ConstructWKTRTree()
	{
		final String spatial_add_node = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/addGeometryWKTToLayer";
		
		String result = p_neo4j_graph_store.Execute("match (a) where has(a."+ longitude_property_name +") return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		//System.out.println(result);
		
		ArrayList<String> l = p_neo4j_graph_store.GetExecuteResultData(result);
		for(int i = 0;i<l.size();i++)
		{
			String record = l.get(i);
			String[] ll = record.split(",");
			int id = Integer.parseInt(ll[0]);
			double latitude = Double.parseDouble(ll[1]);
			double longitude = Double.parseDouble(ll[2]);
			
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
		final String create = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/addSimplePointLayer";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"layer\" : \""+layername+"\", \"lat\" : \"lat\", \"lon\" : \"lon\" }";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	public void CreateSpatialIndex(String layername)
	{
		final String create = SERVER_ROOT_URI + "/index/node/";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"name\" : \""+layername+"\", \"config\" : { \"provider\" : \"spatial\",\"geometry_type\" : \"point\",\"lat\" : \"lat\", \"lon\" : \"lon\"}}";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	
	public void AddSpatialNodesToPointLayer(String layername)
	{
		final String spatial_create_node = SERVER_ROOT_URI + "/node";
		final String spatial_addto_index = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/addNodeToLayer";
		
		String result = p_neo4j_graph_store.Execute("match (a) where has(a." + longitude_property_name + ") return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		
		ArrayList<String> spatial_vertices = p_neo4j_graph_store.GetExecuteResultData(result);
		for(int i = 0;i<spatial_vertices.size();i++)
		{
			String record = spatial_vertices.get(i);
			String[] l = record.split(",");
			String id = l[0];
			double latitude = Double.parseDouble(l[1]);
			double longitude = Double.parseDouble(l[2]);
			
			WebResource resource = Client.create().resource(spatial_create_node);
			String entity = "{ \n \"lat\": "+latitude+", \n \"lon\" : "+longitude+", \n \"id\": "+ id + " \n} ";
			
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
			result = response.getEntity(String.class);
			JSONObject jsonObject = JSONObject.fromObject(result);
			String location = jsonObject.getString("self");
			response.close();
			
			
			l = location.split("/");
			id = l[6];
				
			resource = Client.create().resource(spatial_addto_index);
			entity = "{\"layer\" : \""+layername+"\", \"node\" : \""+SERVER_ROOT_URI+"/node/"+id+"\"}";
			response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
        	result = response.getEntity(String.class);
		}	
	}
	
	public String FindSpatialLayer(String layername)
	{
		final String find_ayer = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/getLayer";
		WebResource resource = Client.create().resource(find_ayer);
		String entity = "{\"layer\" : \"" + layername + "\"}";
		
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		return result;
	}
	
	public void TraversalInReachNodes(int start_id)
	{
		ArrayList<Integer> in_neighbors = p_neo4j_graph_store.GetInNeighbors(start_id);
		for(int i = 0;i<in_neighbors.size();i++)
		{
			int in_neighbor = in_neighbors.get(i);
			if(VisitedNodes.add(in_neighbor))
			{
				TraversalInReachNodes(in_neighbor);
			}
		}
	}
	
	public void TraversalOutReachNodes(int start_id)
	{
		ArrayList<Integer> out_neighbors = p_neo4j_graph_store.GetOutNeighbors(start_id);
		for(int i = 0;i<out_neighbors.size();i++)
		{
			int out_neighbor = out_neighbors.get(i);
			if(VisitedNodes.add(out_neighbor))
			{
				TraversalOutReachNodes(out_neighbor);
			}
		}
	}
	
	public void CreateTransitiveClosureFromOutNeighbor()
	{
		String query = "match (a:Graph_node) where has(a.RMBR_minx) return id(a) limit 100";
		String result = p_neo4j_graph_store.Execute(query);
		System.out.println(result);
		
		ArrayList<String> vertices_with_RMBR = p_neo4j_graph_store.GetExecuteResultData(result);
		for(int i = 0;i<vertices_with_RMBR.size();i++)
		{
			int id = Integer.parseInt(vertices_with_RMBR.get(i));
			VisitedNodes = new HashSet();
			TraversalOutReachNodes(id);
			System.out.println(VisitedNodes);
			/*Iterator iter = VisitedNodes.iterator();
			while(iter.hasNext())
			{
				query = "match (a) where id(a) = "+iter.next()+" set a.reach_nodes = a.reach_nodes + "+id;
				result = p_neo4j_graph_store.Execute(query);
				System.out.println(result);
			}*/
		}
	}
	
	public void UnionReachNodes(int id)
	{
		boolean updated = false;
		
		String query = "match (a) where id(a) = " + id + " return a.reach_nodes";
		
		String result = p_neo4j_graph_store.Execute(query);
		
		ArrayList<String> rows = p_neo4j_graph_store.GetExecuteResultData(result);

		String s_a_reach_nodes = rows.get(0);
		//System.out.println(s_a_reach_nodes);
		result = s_a_reach_nodes.substring(1, s_a_reach_nodes.length()-1);
		String[] l_a_reach_nodes = result.split(",");
		
		query = "match (b) - [] -> (a) where id(a) = " + id + " return id(b), b.reach_nodes";
		result = p_neo4j_graph_store.Execute(query);
		
		rows = p_neo4j_graph_store.GetExecuteResultData(result);
		
		for(int i = 0;i<rows.size();i++)
		{
			String row = rows.get(i);
			//System.out.println(row);
			if(row.contains("null"))
			{
				String[] line = row.split(",");
				String s_id = line[0];
				query = "match (b) where id(b) = " + s_id + " set b.reach_nodes = " + s_a_reach_nodes;
				//System.out.println(query); 
				result = p_neo4j_graph_store.Execute(query);
				
				int i_id = Integer.parseInt(s_id);
				if(!VisitedNodes.contains(i_id))
				{
					queue.add(i_id);
					VisitedNodes.add(i_id);
				}
			}
			else
			{
				HashSet<String> hs_b_reach_nodes = new HashSet();
				row = row.replaceFirst(",", "#");
				String[] line = row.split("#");
				String s_id = line[0];
				String s_b_reach_nodes = line[1];
				s_b_reach_nodes = s_b_reach_nodes.substring(1, s_b_reach_nodes.length()-1);
				StringTokenizer st = new StringTokenizer(s_b_reach_nodes,",");
				while(st.hasMoreTokens())
					hs_b_reach_nodes.add(st.nextToken());
				
				//System.out.println(hs_b_reach_nodes);
				
				query = "match (b) where id(b) = " + s_id + " set b.reach_nodes = b.reach_nodes + [";
				boolean changed = false;
				for(int j = 0; j<l_a_reach_nodes.length; j++)
				{
					String current_id = l_a_reach_nodes[j];
					if(!hs_b_reach_nodes.contains(current_id))
					{
						query = query + current_id + ",";
						changed = true;
					}
				}
				if(changed)
				{
					query = query.substring(0, query.length()-1);
					query += "]";
					//System.out.println(query);
					result = p_neo4j_graph_store.Execute(query);
					
					int i_id = Integer.parseInt(s_id);
					if(!VisitedNodes.contains(i_id))
					{
						queue.add(i_id);
						VisitedNodes.add(i_id);
					}
				}
			}
		}			
	}
	
	public void CreateTransitiveClosureFromInNeighbor()
	{	
		VisitedNodes = new HashSet();
		queue = new LinkedList();
		String query = "match (a) where has(a.longitude) return id(a)";
		String result = p_neo4j_graph_store.Execute(query);
		
		ArrayList<String> spatial_nodes = p_neo4j_graph_store.GetExecuteResultData(result);
		//System.out.println(spatial_nodes);
		for(int i = 0;i<spatial_nodes.size();i++)
		{
			String id = spatial_nodes.get(i);
			//System.out.println(id);
			
			query = "match (b) - [] -> (a)  where id(a) = " + id + " return id(b)";
			result = p_neo4j_graph_store.Execute(query);
			ArrayList<String> in_nodes = p_neo4j_graph_store.GetExecuteResultData(result);
			for(int j = 0;j<in_nodes.size();j++)
			{
				String in_node_id = in_nodes.get(j);
				//System.out.println(in_node_id);
				
				int i_node_id = Integer.parseInt(in_node_id);
				if(!VisitedNodes.contains(i_node_id))
				{
					VisitedNodes.add(Integer.parseInt(in_node_id));
					queue.add(Integer.parseInt(in_node_id));					
				}
				if(!p_neo4j_graph_store.HasProperty(Integer.parseInt(in_node_id), "reach_nodes"))
				{
					query = "match (b) where id(b) = " + in_node_id + " set b.reach_nodes = [" + id + "]";
					//System.out.println(query);
					result = p_neo4j_graph_store.Execute(query);
					//System.out.println(result);
				}
				else
				{
					query = "match (b) where id(b) = " + in_node_id + " set b.reach_nodes = b.reach_nodes + " + id;
					//System.out.println(query);
					result = p_neo4j_graph_store.Execute(query);
					//System.out.println(result);
				}
			}
		}
		
		while(!queue.isEmpty())
		{
			int current_id = queue.poll();
			VisitedNodes.remove(current_id);
			System.out.println(VisitedNodes.size());
			UnionReachNodes(current_id);
		}
		
	}
	
	public void CreateTransitiveClosure()
	{
		ArrayList<Integer> spatial_nodes = p_neo4j_graph_store.GetSpatialVertices();
		for(int i = 0;i<spatial_nodes.size();i++)
		{
			int id = spatial_nodes.get(i);
			VisitedNodes = new HashSet();
			TraversalInReachNodes(id);
			Iterator iter = VisitedNodes.iterator();
			while(iter.hasNext())
			{
				String query = "match (a) where id(a) = "+iter.next()+" set a.reach_nodes = a.reach_nodes + "+id;
				String result = p_neo4j_graph_store.Execute(query);
				System.out.println(result);
			}
		}
	}
	
	public void Preprocess()
	{
		CreatePointLayer("simplepointlayer");
		CreateSpatialIndex("simplepointlayer");
		AddSpatialNodesToPointLayer("simplepointlayer");
	}
	
	public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		HashSet<Integer> hs = RangeQuery("simplepointlayer",rect);
		//System.out.println(hs);
		
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