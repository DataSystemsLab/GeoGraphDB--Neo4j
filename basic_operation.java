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

class Rectangle
{
	double min_x;
	double min_y;
	double max_x;
	double max_y;
}

public class basic_operation {
	
	//execute a cypher query return a json format string
	public static String Execute(String query)
	{
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		
		final String txUri = SERVER_ROOT_URI + "transaction/commit";
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
			l.add(str);
		}
		return l;
	}
	
	//return all vertices' id
	public static ArrayList<Integer> GetAllVertices()
	{
		String query = "match (a) return id(a)";
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
	public static ArrayList<Integer> GetSpatialVertices()
	{
		String query = "match (a) where has (a.latitude) return id(a)";
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
	public static String GetVertexAllAttributes(int id)
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
	public static ArrayList<Integer> GetOutNeighbors(int id)
	{
		String query = "match (a)-[]->(b) where id(a) = " +Integer.toString(id) +" return id(b)";

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
	
	//get all in neighbors of a vertex with its given id
	public static ArrayList<Integer> GetInNeighbors(int id)
	{
		String query = "match (a)-[]->(b) where id(b) = " +Integer.toString(id) +" return id(a)";

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
	
	//given a vertex id and name of its attribute, return the attribute value
	public static String GetVertexAttributeValue(int id, String attributename)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" return a."+attributename;
		
		String result = Execute(query);
		
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");
		str = str.substring(1, str.length()-1);

		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("data");
		str = str.substring(1, str.length()-1);
		
		if(str.equals(""))
			return null;
		
		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("row");
		str = str.substring(1, str.length()-1);
		
		return str;
	}
	
	//add one attribute to a given id vertex
	public static String AddVertexAttribute(int id, String attributename, String value)
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
		
		System.out.println(result);
		return result;
	}
	
	//given a vertex id return a boolean value indicating whether it is a spatial vertex
	public static boolean IsSpatial(int id)
	{
		String lat=null; 
		lat=GetVertexAttributeValue(id, "latitude");
		
		if(lat.equals("null"))
			return false;
		else 
			return true;
	}
	
	// give a vertex id return a boolean value indicating whether it has RMBR
	public static boolean HasRMBR(int id)
	{
		String RMBR_minx=null; 
		RMBR_minx=GetVertexAttributeValue(id, "RMBR_minx");
		
		if(RMBR_minx.equals("null"))
			return false;
		else 
			return true;
	}
	
	//given vertex label and attribute name and value return id of this node(this function requires that this attribute has unique constraint)
	public static int GetVertexID(String label, String attribute, String value)
	{
		String query = "match (a:"+ label +") where a." + attribute + " = \\\"" + value + "\\\" return id(a)";System.out.println(query);
		
		String result = Execute(query);System.out.println(result);
		
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");System.out.println(str);
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
	
	//MBR operation of a given id vertex's current RMBR and another rectangle return a new rectangle, if no change happens it will return null
	public static Rectangle MBR(int id,String minx2_s, String miny2_s, String maxx2_s, String maxy2_s)
	{	
		Rectangle rec = new Rectangle();				
			
		if(!HasRMBR(id))
		{			
			rec.min_x = Double.parseDouble(minx2_s);
			rec.min_y = Double.parseDouble(miny2_s);
			rec.max_x = Double.parseDouble(maxx2_s);
			rec.max_y = Double.parseDouble(maxy2_s);
			return rec;
		}
		
		String minx1_s = GetVertexAttributeValue(id, "RMBR_minx");
		String miny1_s = GetVertexAttributeValue(id, "RMBR_miny");
		String maxx1_s = GetVertexAttributeValue(id, "RMBR_maxx");
		String maxy1_s = GetVertexAttributeValue(id, "RMBR_maxy");
		
		rec.min_x = Double.parseDouble(minx1_s);
		rec.min_y = Double.parseDouble(miny1_s);
		rec.max_x = Double.parseDouble(maxx1_s);
		rec.max_y = Double.parseDouble(maxy1_s);
		
		
		double minx2 = Double.parseDouble(minx2_s);
		double miny2 = Double.parseDouble(miny2_s);
		double maxx2 = Double.parseDouble(maxx2_s);
		double maxy2 = Double.parseDouble(maxy2_s);
		
		boolean flag = false;
		if(minx2 < rec.min_x)
		{
			rec.min_x = minx2;
			flag = true;
		}
		
		if(miny2 < rec.min_y)
		{
			rec.min_y = miny2;
			flag = true;
		}
		
		if(maxx2 > rec.max_x)
		{
			rec.max_x = maxx2;
			flag = true;
		}
		
		if(maxy2 > rec.max_y)
		{
			rec.max_y = maxy2;
			flag = true;
		}
		
		if(flag)
			return rec;
		else
			return null;
	}
	
	
	
	/*public static void Construct_RMBR()
	{
		ArrayList<Integer> vertices = GetAllVertices();
		
		for(int i = 0;i<vertices.size();i++)
		{
			int id = vertices.get(i);
			AddVertexAttribute(id, "RMBR_minx","-1");
			AddVertexAttribute(id, "RMBR_miny","-1");
			AddVertexAttribute(id, "RMBR_maxx","-1");
			AddVertexAttribute(id, "RMBR_maxy","-1");
		}
		
		ArrayList<Integer> spatial_vertices = GetSpatialVertices();
		Queue<Integer> queue = new LinkedList<Integer>();
		for(int i = 0;i<spatial_vertices.size();i++)
			queue.add(spatial_vertices.get(i));
		
		while(!queue.isEmpty())
		{
			int current_id = queue.poll();
			
			ArrayList<Integer> neighbors = GetInNeighbors(current_id);
			
			String latitude = null, longitude = null;
			
			String minx_s = null, miny_s = null, maxx_s = null, maxy_s = null;
			minx_s = GetVertexAttributeValue(current_id, "RMBR_minx");
			miny_s = GetVertexAttributeValue(current_id, "RMBR_miny");
			maxx_s = GetVertexAttributeValue(current_id, "RMBR_maxx");
			maxy_s = GetVertexAttributeValue(current_id, "RMBR_maxy");
			
			boolean isspatial = false;
			
			if(IsSpatial(current_id))
			{
				isspatial = true;
				
				latitude = GetVertexAttributeValue(current_id, "latitude");
				longitude = GetVertexAttributeValue(current_id, "longitude");
			}
			
			for(int i = 0;i<neighbors.size();i++)
			{
				int neighbor = neighbors.get(i);
				boolean changed = false;
				
				if(isspatial)
				{
					Rectangle new_RMBR = MBR(neighbor, longitude, latitude, longitude, latitude);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						 
						AddVertexAttribute(neighbor, "RMBR_minx", minx);
						AddVertexAttribute(neighbor, "RMBR_miny", miny);
						AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
						AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
					}
				}
				
				Rectangle new_RMBR = MBR(neighbor, minx_s, miny_s, maxx_s, maxy_s);
				if(new_RMBR != null)
				{
					changed = true;
					
					String minx = Double.toString(new_RMBR.min_x);
					String miny = Double.toString(new_RMBR.min_y);
					String maxx = Double.toString(new_RMBR.max_x);
					String maxy = Double.toString(new_RMBR.max_y);
					 
					AddVertexAttribute(neighbor, "RMBR_minx", minx);
					AddVertexAttribute(neighbor, "RMBR_miny", miny);
					AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
					AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
				}
				
				if(changed)
					queue.add(neighbor);
			}		
		}
	}*/
	
	public static boolean Location_In_Rect(double lat, double lon, Rectangle rect)
	{
		if(lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x)
			return false;
		else 
			return true;
	}
	
	/*public static boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		Rectangle RMBR = new Rectangle();
		String minx_s = GetVertexAttributeValue(start_id, "RMBR_minx");
		
		if(minx_s.equals("-1"))
			return false;
		
		String miny_s = GetVertexAttributeValue(start_id, "RMBR_miny");
		String maxx_s = GetVertexAttributeValue(start_id, "RMBR_maxx");
		String maxy_s = GetVertexAttributeValue(start_id, "RMBR_maxy");
		
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
			return false;
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
			return true;
		
		ArrayList<Integer> outneighbors = GetOutNeighbors(start_id);
		
		for(int i = 0;i<outneighbors.size();i++)
		{
			int outneighbor = outneighbors.get(i);
			
			if(IsSpatial(outneighbor))
			{
				double lat = Double.parseDouble(GetVertexAttributeValue(outneighbor, "latitude"));
				double lon = Double.parseDouble(GetVertexAttributeValue(outneighbor, "longitude"));
				if(Location_In_Rect(lat, lon, rect))
					return true;
			}
			
			boolean result = ReachabilityQuery(outneighbor, rect);
			if(result)
				return true;
			else
				return false;
		}
		
		return false;
		
	} */
	
}