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

public class GeoReach	{
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	static basic_operation p_basic_operation = new basic_operation();
	
	public static void Construct_RMBR()
	{	
		ArrayList<Integer> spatial_vertices = p_basic_operation.GetSpatialVertices();
		ArrayList<Integer> queue = new ArrayList<Integer>();
		for(int i = 0;i<spatial_vertices.size();i++)
			queue.add(spatial_vertices.get(i));
		
		while(!queue.isEmpty())
		{
			System.out.println(queue.size());
			int current_id = queue.get(0);
			queue.remove(0);
			
			ArrayList<Integer> neighbors = p_basic_operation.GetInNeighbors(current_id);
			
			String latitude = null, longitude = null;
			
			String minx_s = null, miny_s = null, maxx_s = null, maxy_s = null;
			
			boolean isspatial = false;
			
			if(p_basic_operation.IsSpatial(current_id))
			{
				isspatial = true;
				
				latitude = p_basic_operation.GetVertexAttributeValue(current_id, "latitude");
				longitude = p_basic_operation.GetVertexAttributeValue(current_id, "longitude");
			}
			
			boolean hasRMBR = false;
			
			if(p_basic_operation.HasRMBR(current_id))
			{
				hasRMBR = true;
				
				minx_s = p_basic_operation.GetVertexAttributeValue(current_id, "RMBR_minx");
				miny_s = p_basic_operation.GetVertexAttributeValue(current_id, "RMBR_miny");
				maxx_s = p_basic_operation.GetVertexAttributeValue(current_id, "RMBR_maxx");
				maxy_s = p_basic_operation.GetVertexAttributeValue(current_id, "RMBR_maxy");
			}
			
			for(int i = 0;i<neighbors.size();i++)
			{
				int neighbor = neighbors.get(i);
				boolean changed = false;
				
				if(isspatial)
				{
					Rectangle new_RMBR = p_basic_operation.MBR(neighbor, longitude, latitude, longitude, latitude);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						 
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_minx", minx);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_miny", miny);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
					}
				}
				
				if(hasRMBR)
				{
					Rectangle new_RMBR = p_basic_operation.MBR(neighbor, minx_s, miny_s, maxx_s, maxy_s);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						 
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_minx", minx);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_miny", miny);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
						p_basic_operation.AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
					}
				}
					
				if(changed&&!queue.contains(neighbor))
				{
					queue.add(neighbor);
					System.out.println(queue.size());
					System.out.println("Added");
				}
			}		
		}
	}
	
	public static boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		VisitedVertices.add(start_id);
		
		if(!p_basic_operation.HasRMBR(start_id))
			return false;
		
		Rectangle RMBR = new Rectangle();
		String minx_s = p_basic_operation.GetVertexAttributeValue(start_id, "RMBR_minx");
		String miny_s = p_basic_operation.GetVertexAttributeValue(start_id, "RMBR_miny");
		String maxx_s = p_basic_operation.GetVertexAttributeValue(start_id, "RMBR_maxx");
		String maxy_s = p_basic_operation.GetVertexAttributeValue(start_id, "RMBR_maxy");
		
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
			return false;
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
			return true;
		
		ArrayList<Integer> outneighbors = p_basic_operation.GetOutNeighbors(start_id);
		
		for(int i = 0;i<outneighbors.size();i++)
		{
			int outneighbor = outneighbors.get(i);
			
			if(p_basic_operation.IsSpatial(outneighbor))
			{
				double lat = Double.parseDouble(p_basic_operation.GetVertexAttributeValue(outneighbor, "latitude"));
				double lon = Double.parseDouble(p_basic_operation.GetVertexAttributeValue(outneighbor, "longitude"));
				if(p_basic_operation.Location_In_Rect(lat, lon, rect))
					return true;
			}
			
			if(VisitedVertices.contains(outneighbor))
				continue;
			
			boolean result = ReachabilityQuery(outneighbor, rect);
			if(result)
				return true;
		}
		
		return false;
		
	}
}