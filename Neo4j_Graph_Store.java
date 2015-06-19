package def;

import java.util.*;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.*;
import java.net.URI;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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
	}
	
	private String SERVER_ROOT_URI;
	private String longitude_property_name;
	private String latitude_property_name;
	
	//execute a cypher query return a json format string
	public String Execute(String query)
	{		
		final String txUri = SERVER_ROOT_URI + "/transaction/commit";
		WebResource resource = Client.create().resource( txUri );
		
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
	public static HashSet<Integer> GetExecuteResultDataInSet(String result)
	{		
		HashSet<Integer> hs  = new HashSet<Integer>();
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject) jsonParser.parse(result);
		
		JsonArray jsonArr = (JsonArray) jsonObject.get("results");
		
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
		String result = Execute(query);

		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");
		str = str.substring(1, str.length()-1);

		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("data");

		JSONArray arr = JSONArray.fromObject(str);
		ArrayList l  = new ArrayList<Integer>();
		
		for(int i = 0;i<arr.size();i++)
		{
			jsonObject=arr.getJSONObject(i);
			str = jsonObject.getString("row");
			str = str.substring(1, str.length()-1);
			l.add(Integer.parseInt(str));
		}
		return l;
	}
	
	//return all spatial vertices' id
	public ArrayList<Integer> GetSpatialVertices()
	{
		String query = "match (a:Graph_node) where has (a."+ longitude_property_name +") return id(a)";
		String result = Execute(query);

		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");
		str = str.substring(1, str.length()-1);

		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("data");

		JSONArray arr = JSONArray.fromObject(str);
		ArrayList l  = new ArrayList<Integer>();
		
		for(int i = 0;i<arr.size();i++)
		{
			jsonObject=arr.getJSONObject(i);
			str = jsonObject.getString("row");
			str = str.substring(1, str.length()-1);
			l.add(Integer.parseInt(str));
		}
		return l;
	}
	
	//get all attributes as json format string by a given id
	public String GetVertexAllAttributes(int id)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" return a";
		
		String result = Execute(query);
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");
		str = str.substring(1, str.length()-1);
		
		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("data");
		str = str.substring(1, str.length()-1);

		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("row");
		str = str.substring(1, str.length()-1);
		return str;
	}
	
	//get all out neighbors of a vertex with its given id
	public ArrayList<Integer> GetOutNeighbors(int id)
	{
		ArrayList<Integer> l = new ArrayList<Integer>();
		
		String query = "match (a)-[]->(b) where id(a) = " +Integer.toString(id) +" return id(b)";

		String result = Execute(query);
		
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

		String result = Execute(query);
		
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
		
		String result = Execute(query);
		
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
		ArrayList<String> result = GetExecuteResultData(Execute(query));
		
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
		String result = Execute(query);
		
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("errors");
				
		str = str.substring(1, str.length()-1);
		
		if(str.equals(""))
			result = "Succeed!";
		else
			result = str;
		
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
		
		String result = Execute(query);
		
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");
		str = str.substring(1, str.length()-1);

		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("data");
		str = str.substring(1, str.length()-1);
		
		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("row");
		str = str.substring(1, str.length()-1);
		
		int id = Integer.parseInt(str);
		return id;
	}
		
	
	public static boolean Location_In_Rect(double lat, double lon, Rectangle rect)
	{
		if(lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x)
			return false;
		else 
			return true;
	}
	
}