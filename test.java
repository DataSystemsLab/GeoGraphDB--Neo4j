package def;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/*import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;*/




import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

class Rectangle
{
	double min_x;
	double min_y;
	double max_x;
	double max_y;
}

public class test {

	public static void main(String[] args) {

		//System.out.println(GetVertexAllAttributes(0));
		/*ArrayList l = GetInNeighbors(0);
		for(int i = 0;i<l.size();i++)
		{
			String str = String.valueOf(l.get(i));
			System.out.println(str);
		}*/
		//System.out.println(GetVertexAttributeValue(4, "name"));
		//System.out.println(AddVertexAttribute(4,"language","Chinese"));
		
		/*ArrayList l = GetAllVertices();
		for(int i = 0;i<l.size();i++)
		{
			String str = String.valueOf(l.get(i));
			System.out.println(str);
		}
		
		if(IsSpatial(0))
			System.out.println("Yes");
		else
			System.out.println("No");*/
		
		ArrayList l = GetSpatialVertices();
		for(int i = 0;i<l.size();i++)
		{
			String str = String.valueOf(l.get(i));
			System.out.println(str);
		}
		
	}
	
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
	
	public static ArrayList<Integer> GetSpatialVertices()
	{
		String query = "match (a) where has (a.lat) return id(a)";
		String result = Execute(query);System.out.println(result);

		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("results");System.out.println(str);
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
		
		jsonObject = JSONObject.fromObject(str);
		str = jsonObject.getString("row");
		str = str.substring(1, str.length()-1);
		
		return str;
	}
	
	public static String AddVertexAttribute(int id, String attributename, String value)
	{
		String query = "match (a) where id(a) = " +Integer.toString(id) +" set a."+attributename+"="+"\\\""+value+"\\\"";
		String result = Execute(query);
		
		JSONObject jsonObject = JSONObject.fromObject(result);
		String str = jsonObject.getString("errors");
		
		System.out.println(str);
		
		str = str.substring(1, str.length()-1);
		
		System.out.println(str);
		
		if(str.equals(""))
			result = "Succeed!";
		else
			result = str;
		
		return result;
	}

	public static boolean IsSpatial(int id)
	{
		String lat=null; 
		lat=GetVertexAttributeValue(id, "lat");
		
		if(lat.equals("null"))
			return false;
		else 
			return true;
	}
	
	public static Rectangle MBR(int id,int out_neighbor_id)
	{
		Rectangle rec = new Rectangle();
		
		String minx1_s = GetVertexAttributeValue(id,"RMBR_minx");
		String minx2_s = GetVertexAttributeValue(out_neighbor_id, "RMBR_minx")
		
		if(minx1_s == "-1"&&minx2_s == "1")
			return null;
			
		if(minx1_s == "-1")
		{			
			rec.min_x = Double.parseDouble(minx2_s);
			rec.min_y = Double.parseDouble(miny2_s);
			rec.max_x = Double.parseDouble(maxx2_s);
			rec.max_y = Double.parseDouble(maxy2_s);
			return rec;
		}
		
		if(minx2_s == "-1")
			return null;
		
		String miny1_s = GetVertexAttributeValue(id, "RMBR_miny");
		String maxx1_s = GetVertexAttributeValue(id, "RMBR_maxx");
		String maxy1_s = GetVertexAttributeValue(id, "RMBR_maxy");
		
		rec.min_x = Double.parseDouble(minx1_s);
		rec.min_y = Double.parseDouble(GetVertexAttributeValue(id, "RMBR_miny"));
		rec.max_x = Double.parseDouble(GetVertexAttributeValue(id, "RMBR_maxx"));
		rec.max_y = Double.parseDouble(GetVertexAttributeValue(id, "RMBR_maxy"));
		
		
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
	
	public static void Construct_RMBR()
	{
		boolean changed = true;
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
			
			if(IsSpatial(current_id))
			{
				Rectangle rec = new Rectangle();
			}
			
			
			
		}

	}
}
