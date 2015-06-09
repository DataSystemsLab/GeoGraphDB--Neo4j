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

public class GeoReach implements ReachabilityQuerySolver	{
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
	
	// give a vertex id return a boolean value indicating whether it has RMBR
	public static boolean HasRMBR(int id)
	{
		String RMBR_minx=null; 
		RMBR_minx = p_neo4j_graph_store.GetVertexAttributeValue(id, "RMBR_minx");
		
		if(RMBR_minx.equals("null"))
			return false;
		else 
			return true;
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
		
		String minx1_s = p_neo4j_graph_store.GetVertexAttributeValue(id, "RMBR_minx");
		String miny1_s = p_neo4j_graph_store.GetVertexAttributeValue(id, "RMBR_miny");
		String maxx1_s = p_neo4j_graph_store.GetVertexAttributeValue(id, "RMBR_maxx");
		String maxy1_s = p_neo4j_graph_store.GetVertexAttributeValue(id, "RMBR_maxy");
		
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
	
	public void Preprocess()
	{	
		ArrayList<Integer> spatial_vertices = p_neo4j_graph_store.GetSpatialVertices();
		ArrayList<Integer> queue = new ArrayList<Integer>();
		for(int i = 0;i<spatial_vertices.size();i++)
			queue.add(spatial_vertices.get(i));
		
		while(!queue.isEmpty())
		{
			System.out.println(queue.size());
			int current_id = queue.get(0);
			queue.remove(0);
			
			ArrayList<Integer> neighbors = p_neo4j_graph_store.GetInNeighbors(current_id);
			
			String latitude = null, longitude = null;
			
			String minx_s = null, miny_s = null, maxx_s = null, maxy_s = null;
			
			boolean isspatial = false;
			
			if(p_neo4j_graph_store.IsSpatial(current_id))
			{
				isspatial = true;
				
				latitude = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "latitude");
				longitude = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "longitude");
			}
			
			boolean hasRMBR = false;
			
			if(HasRMBR(current_id))
			{
				hasRMBR = true;
				
				minx_s = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "RMBR_minx");
				miny_s = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "RMBR_miny");
				maxx_s = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "RMBR_maxx");
				maxy_s = p_neo4j_graph_store.GetVertexAttributeValue(current_id, "RMBR_maxy");
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
						 
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_minx", minx);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_miny", miny);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
					}
				}
				
				if(hasRMBR)
				{
					Rectangle new_RMBR = MBR(neighbor, minx_s, miny_s, maxx_s, maxy_s);
					if(new_RMBR != null)
					{
						changed = true;
						
						String minx = Double.toString(new_RMBR.min_x);
						String miny = Double.toString(new_RMBR.min_y);
						String maxx = Double.toString(new_RMBR.max_x);
						String maxy = Double.toString(new_RMBR.max_y);
						 
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_minx", minx);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_miny", miny);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_maxx", maxx);
						p_neo4j_graph_store.AddVertexAttribute(neighbor, "RMBR_maxy", maxy);
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
		
		if(!HasRMBR(start_id))
			return false;
		
		Rectangle RMBR = new Rectangle();
		String minx_s = p_neo4j_graph_store.GetVertexAttributeValue(start_id, "RMBR_minx");
		String miny_s = p_neo4j_graph_store.GetVertexAttributeValue(start_id, "RMBR_miny");
		String maxx_s = p_neo4j_graph_store.GetVertexAttributeValue(start_id, "RMBR_maxx");
		String maxy_s = p_neo4j_graph_store.GetVertexAttributeValue(start_id, "RMBR_maxy");
		
		RMBR.min_x = Double.parseDouble(minx_s);
		RMBR.min_y = Double.parseDouble(miny_s);
		RMBR.max_x = Double.parseDouble(maxx_s);
		RMBR.max_y = Double.parseDouble(maxy_s);
		
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
			return false;
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
			return true;
		
		ArrayList<Integer> outneighbors = p_neo4j_graph_store.GetOutNeighbors(start_id);
		
		for(int i = 0;i<outneighbors.size();i++)
		{
			int outneighbor = outneighbors.get(i);
			
			if(p_neo4j_graph_store.IsSpatial(outneighbor))
			{
				double lat = Double.parseDouble(p_neo4j_graph_store.GetVertexAttributeValue(outneighbor, "latitude"));
				double lon = Double.parseDouble(p_neo4j_graph_store.GetVertexAttributeValue(outneighbor, "longitude"));
				if(p_neo4j_graph_store.Location_In_Rect(lat, lon, rect))
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