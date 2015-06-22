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
		return p_neo4j_graph_store.HasProperty(id, "RMBR_minx");
	}
	
	public static Rectangle GetRMBR(int id)
	{
		Rectangle RMBR = new Rectangle();
		
		String query = "match (a) where id(a) = " + id + " return a.RMBR_minx, a.RMBR_miny, a.RMBR_maxx, a.RMBR_maxy";		
		ArrayList<String> result = p_neo4j_graph_store.GetExecuteResultData(p_neo4j_graph_store.Execute(query));
		
		String data = result.get(0);
		String[] l = data.split(",");
		
		RMBR.min_x = Double.parseDouble(l[0]);
		RMBR.min_y = Double.parseDouble(l[1]);
		RMBR.max_x = Double.parseDouble(l[2]);
		RMBR.max_y = Double.parseDouble(l[3]);
		return RMBR;
	}
	
	//MBR operation of a given id vertex's current RMBR and another rectangle return a new rectangle, if no change happens it will return null
	public static Rectangle MBR(int id,String minx2_s, String miny2_s, String maxx2_s, String maxy2_s)
	{	
		Rectangle rec;				
			
		if(!HasRMBR(id))
		{	
			rec = new Rectangle();
			rec.min_x = Double.parseDouble(minx2_s);
			rec.min_y = Double.parseDouble(miny2_s);
			rec.max_x = Double.parseDouble(maxx2_s);
			rec.max_y = Double.parseDouble(maxy2_s);
			return rec;
		}

		rec = GetRMBR(id);	
		
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
		Queue<Integer> queue = new LinkedList<Integer>();
		HashSet<Integer> hs = new HashSet();
		for(int i = 0;i<spatial_vertices.size();i++)
		{
			int id = spatial_vertices.get(i);
			queue.add(id);
			hs.add(id);
		}
		
		while(!queue.isEmpty())
		{
			System.out.println(hs.size());
			//System.out.println(queue);
			int current_id = queue.poll();
			hs.remove(current_id);
			
			ArrayList<Integer> neighbors = p_neo4j_graph_store.GetInNeighbors(current_id);
			
			String latitude = null, longitude = null;
			
			String minx_s = null, miny_s = null, maxx_s = null, maxy_s = null;
			
			boolean isspatial = false;
			
			if(p_neo4j_graph_store.IsSpatial(current_id))
			{
				isspatial = true;
				
				double[] location = p_neo4j_graph_store.GetVerticeLocation(current_id);
				longitude = String.valueOf(location[0]);
				latitude = String.valueOf(location[1]);
			}
			
			boolean hasRMBR = false;
			
			if(HasRMBR(current_id))
			{
				hasRMBR = true;
				
				Rectangle p_rec = GetRMBR(current_id);
				minx_s = String.valueOf(p_rec.min_x);
				miny_s = String.valueOf(p_rec.min_y);
				maxx_s = String.valueOf(p_rec.max_x);
				maxy_s = String.valueOf(p_rec.max_y);
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
						
						String query = "match (a) where id(a) = " + neighbor + " set a.RMBR_minx = " + minx + ", a.RMBR_miny = " + miny + ", a.RMBR_maxx = " + maxx + ", a.RMBR_maxy = " + maxy;
						p_neo4j_graph_store.Execute(query);
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
						
						String query = "match (a) where id(a) = " + neighbor + " set a.RMBR_minx = " + minx + ", a.RMBR_miny = " + miny + ", a.RMBR_maxx = " + maxx + ", a.RMBR_maxy = " + maxy;
						p_neo4j_graph_store.Execute(query);
					}
				}
					
				if(changed&&!hs.contains(neighbor))
				{
					queue.add(neighbor);
					hs.add(neighbor);
				}
				
			}	
		}
	}
	
	/*public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		if(!HasRMBR(start_id))
			return false;
		
		Rectangle RMBR = GetRMBR(start_id);
		if(RMBR.min_x > rect.max_x || RMBR.max_x < rect.min_x || RMBR.min_y > rect.max_y || RMBR.max_y < rect.min_y)
			return false;
		
		if(RMBR.min_x > rect.min_x && RMBR.max_x < rect.max_x && RMBR.min_y > rect.min_x && RMBR.max_y < rect.max_y)
			return true;
		
		Queue<Integer> queue = new LinkedList();
		VisitedVertices.clear();
		ArrayList<Integer> outneighbors = p_neo4j_graph_store.GetOutNeighbors(start_id);
		
		for(int i = 0;i<outneighbors.size();i++)
		{
			int outneighbor = outneighbors.get(i);
			
			if(p_neo4j_graph_store.IsSpatial(outneighbor))
			{
				double[] location = p_neo4j_graph_store.GetVerticeLocation(outneighbor);
				
				double lat = location[1];
				double lon = location[0];
				if(p_neo4j_graph_store.Location_In_Rect(lat, lon, rect))
					return true;
			}
			
			boolean result = ReachabilityQuery(outneighbor, rect);
			if(result)
				return true;
	}*/
	
	public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		VisitedVertices.add(start_id);
		
		if(!HasRMBR(start_id))
			return false;
		
		Rectangle RMBR = new Rectangle();
		
		
		Rectangle p_rec = GetRMBR(start_id);
		String minx_s = String.valueOf(p_rec.min_x);
		String miny_s = String.valueOf(p_rec.min_y);
		String maxx_s = String.valueOf(p_rec.max_x);
		String maxy_s = String.valueOf(p_rec.max_y);
		
		
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
				double[] location = p_neo4j_graph_store.GetVerticeLocation(outneighbor);
				
				double lat = location[1];
				double lon = location[0];
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