package def;

import java.util.*;
import java.io.*;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Index {
	
	public String RangeQuery()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String range_query = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/findGeometriesInBBox";
		
		WebResource resource = Client.create().resource(range_query);
		String entity = "{ \"layer\": \"geom\", \"minx\": 0, \"maxx\":90, \"miny\": 0, \"maxy\": 90 }";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		response.close();
		return result;
	}
	
	public void ConstructWKTRTree()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String spatial_add_node = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/addGeometryWKTToLayer";
		
		basic_operation bo = new basic_operation();
		String result = bo.Execute("match (a) where has(a.latitude) return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		//System.out.println(result);
		
		ArrayList<String> l = bo.GetExecuteResultData(result);
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
	
	public void CreatePointLayer()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String create = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/addSimplePointLayer";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"layer\" : \"simplepointlayer\", \"lat\" : \"lat\", \"lon\" : \"lon\" }";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	public void CreateSpatialIndex()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String create = SERVER_ROOT_URI + "index/node/";
		WebResource resource = Client.create().resource(create);
		String entity = "{\"name\" : \"simplepointlayer\", \"config\" : { \"provider\" : \"spatial\",\"geometry_type\" : \"point\",\"lat\" : \"lat\", \"lon\" : \"lon\"}}";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);
	}
	
	public void CreatePoints()
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String spatial_create_node = SERVER_ROOT_URI + "node";
		
		basic_operation bo = new basic_operation();
		String result = bo.Execute("match (a) where has(a.latitude) return id(a) as id, a.latitude as latitude, a.longitude as longitude");
		//System.out.println(result);
		
		try {
			PrintWriter writer = new PrintWriter("/home/yuhansun/data/SetLabel/location.txt","UTF-8");
			ArrayList<String> l = bo.GetExecuteResultData(result);
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
				String entity = "{ \n \"lat\": "+latitude+", \n \"long\" : "+longitude+", \n \"id\": "+ id + " \n} ";
				
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
	
	public void AddPointsToLayer()
	{
		
	}
}