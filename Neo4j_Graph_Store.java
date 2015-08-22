package def;

import java.util.*;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.*;
import java.net.URI;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class Neo4j_Graph_Store implements Graph_Store_Operation{
	
	public Neo4j_Graph_Store()
	{
		Config config = new Config();
		SERVER_ROOT_URI = config.GetServerRoot();
		longitude_property_name = config.GetLongitudePropertyName();
		latitude_property_name = config.GetLatitudePropertyName();
		
		final String txUri = SERVER_ROOT_URI + "/transaction/commit";
		resource = Client.create().resource( txUri );
	}
	
	private String SERVER_ROOT_URI;
	private String longitude_property_name;
	private String latitude_property_name;
	private WebResource resource;
	
	//create web resource and this step is time consuming
	public WebResource GetCypherResource()
	{
		return resource;
	}
	
	public WebResource GetRangeQueryResource()
	{
		final String range_query = SERVER_ROOT_URI + "/ext/SpatialPlugin/graphdb/findGeometriesInBBox";
		WebResource resource = Client.create().resource(range_query);
		return resource;
	}
	
	public static String StartMyServer(String datasource)
	{
		String command = "/home/yuhansun/Documents/Real_data/" + datasource+"/neo4j-community-2.2.3/bin/neo4j start";
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();  		
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String StartServer(String database_path)
	{
		String command = database_path+"/bin/neo4j start";
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();  		
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String StopMyServer(String datasource)
	{
		String command = "/home/yuhansun/Documents/Real_data/" + datasource+"/neo4j-community-2.2.3/bin/neo4j stop";
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();  		
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String StopServer(String database_path)
	{
		String command = database_path+"/bin/neo4j stop";
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();  		
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	//execute a cypher query return a json format string
	public static String Execute(WebResource resource, String query)
	{				
		String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
		ClientResponse response = resource
		        .accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( payload )
		        .post( ClientResponse.class );

		String result = response.getEntity(String.class);
		response.close();
		return result;
	}
	
	public String Execute(String query)
	{		
		String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
		ClientResponse response = resource
		        .accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( payload )
		        .post( ClientResponse.class );

		String result = response.getEntity(String.class);
		response.close();
		return result;
	}
	
	//decode return value of function Execute(String query) to get "data" section
	public static ArrayList<String> GetExecuteResultData(String result)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		
		jsonObject = (JsonObject) jsonArr.get(0);
		
		jsonArr = (JsonArray) jsonObject.get("data");
		ArrayList<String> l  = new ArrayList<String>();
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject=(JsonObject) jsonArr.get(i);
			String str = jsonObject.get("row").toString();
			str = str.substring(1, str.length()-1);
			l.add(str);
		}
		return l;
	}
	
	//decode return value of function Execute(String query) to get "data" section
	public static JsonArray GetExecuteResultDataASJsonArray(String result)
	{		
		HashSet<Integer> hs  = new HashSet<Integer>();
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		
		if(jsonArr.size() == 0)
		{
			System.out.println(result);
			return null;
		}
		
		jsonObject = (JsonObject) jsonArr.get(0);
		
		jsonArr = (JsonArray) jsonObject.get("data");
		
		return jsonArr;
	}	
	
	//decode return value of function Execute(String query) to get "data" section
	public static HashSet<Integer> GetExecuteResultDataInSet(String result)
	{		
		HashSet<Integer> hs  = new HashSet<Integer>();
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		
		if(jsonArr.size() == 0)
			return null;

		jsonObject = (JsonObject) jsonArr.get(0);
		
		jsonArr = (JsonArray) jsonObject.get("data");
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			JsonObject jsonOb = (JsonObject) jsonArr.get(i);
			String row = jsonOb.get("row").toString();
			row = row.substring(1, row.length()-1);
			hs.add(Integer.parseInt(row));
		}
		return hs;
	}	
	
	//return all vertices' id
	public ArrayList<Integer> GetAllVertices()
	{
		String query = "match (a:Graph_node) return id(a)";
		String result = Execute(resource, query);

		ArrayList l  = new ArrayList<Integer>();
		
		return l;
	}
	
	//return all spatial vertices' id
	public ArrayList<Integer> GetSpatialVertices()
	{
		String query = "match (a:Graph_node) where has (a."+ longitude_property_name +") return id(a)";
		String result = Execute(resource, query);
		
		HashSet<Integer> hs = GetExecuteResultDataInSet(result);
		
		ArrayList l  = new ArrayList<Integer>();
		
		Iterator<Integer> iter = hs.iterator();
		while(iter.hasNext())
		{
			l.add(iter.next());
		}		
		
		return l;
	}
	
	//get all attributes as json format string by a given id
	public JsonObject GetVertexAllAttributes(int id)
	{		
		String query = "match (a) where id(a) = " +Integer.toString(id) +" return a";
		
		String result = Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		jsonObject = (JsonObject)jsonArr.get(0);
		jsonArr = (JsonArray)jsonObject.get("row");
		
		jsonObject = (JsonObject)jsonArr.get(0);
		
		return jsonObject;
	}
	
	public JsonArray GetVertexIDandAllAttributes(int id)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" return id(a), a";
		
		String result = Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		jsonObject = (JsonObject)jsonArr.get(0);
		jsonArr = (JsonArray)jsonObject.get("row");	
		
		return jsonArr;
	}
	
	//get all out neighbors of a vertex with its given id
	public ArrayList<Integer> GetOutNeighbors(int id)
	{
		ArrayList<Integer> l = new ArrayList<Integer>();
		
		String query = "match (a)-[]->(b) where id(a) = " +Integer.toString(id) +" return id(b)";

		String result = Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject = (JsonObject) jsonArr.get(i);
			JsonElement jsonElement = (JsonElement) jsonObject.get("row");
			String  row = jsonElement.toString();
			row = row.substring(1, row.length()-1);
			l.add(Integer.parseInt(row));		
		}
		return l;
	}
	
	//get all in neighbors of a vertex with its given id
	public ArrayList<Integer> GetInNeighbors(int id)
	{
		ArrayList<Integer> l = new ArrayList<Integer>();
		
		String query = "match (a)-[]->(b) where id(b) = " +Integer.toString(id) +" return id(a)";

		String result = Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArr.get(0);
		jsonArr = (JsonArray) jsonObject.get("data");
		
		for(int i = 0;i<jsonArr.size();i++)
		{
			jsonObject = (JsonObject) jsonArr.get(i);
			JsonElement jsonElement = (JsonElement) jsonObject.get("row");
			String row = jsonElement.toString();
			row = row.substring(1, row.length()-1);
			l.add(Integer.parseInt(row));		
		}
		return l;
	}
	
	//given a vertex id and name of its attribute, return the attribute value
	public String GetVertexAttributeValue(int id, String attributename)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" return a."+attributename;
		
		String result = Execute(resource, query);
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray errors = (JsonArray) jsonObject.get("errors");
		if(!errors.toString().equals("[]"))
			return null;
		
		JsonArray jsonArray = (JsonArray) jsonObject.get("results");
		jsonObject = (JsonObject) jsonArray.get(0);
		jsonArray = (JsonArray) jsonObject.get("data");
		jsonObject = (JsonObject) jsonArray.get(0);
		String data = jsonObject.toString();
		
		String row = jsonObject.get("row").toString();
		row = row.substring(1, row.length()-1);
		
		if(row.equals("null"))
			return null;
		
		return row;
	}
	
	public double[] GetVerticeLocation(int id)
	{
		double[] location = new double[2];
		String query = "match (a) where id(a) = " + id + " return a.longitude, a.latitude";
		ArrayList<String> result = GetExecuteResultData(Execute(resource, query));
		
		String data = result.get(0);
		String[] l = data.split(",");
		location[0] = Double.parseDouble(l[0]);
		location[1] = Double.parseDouble(l[1]);
		
		return location;
	}
	
	//add one attribute to a given id vertex
	public String AddVertexAttribute(int id, String attributename, String value)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" set a."+attributename+"="+value;
		String result = Execute(resource, query);
		
		return result;
	}
	
	public boolean HasProperty(int id, String propertyname)
	{
		String value = GetVertexAttributeValue(id, propertyname);
		if(value == null)
			return false;
		else
			return true;
	}
	
	//given a vertex id return a boolean value indicating whether it is a spatial vertex
	public boolean IsSpatial(int id)
	{
		boolean has = HasProperty(id, "latitude");
		
		return has;
	}
	
	
	//given vertex label and attribute name and value return id of this node(this function requires that this attribute has unique constraint)
	public int GetVertexID(String label, String attribute, String value)
	{
		String query = "match (a:"+ label +") where a." + attribute + " = " + value + " return id(a)";
		
		String result = Execute(resource, query);
		
		HashSet<Integer> hs = GetExecuteResultDataInSet(result);
		
		Iterator<Integer> iter = hs.iterator();
		
		int id = iter.next();
		return id;
	}
		
	
	public static boolean Location_In_Rect(double lat, double lon, MyRectangle rect)
	{
		if(lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x)
			return false;
		else 
			return true;
	}
	
}