package def;

import java.util.*;
import java.io.*;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Traversal {
	
	//used in query procedure in order to record visited vertices
	public static Set<Integer> VisitedVertices = new HashSet();
	
	static basic_operation p_basic_operation = new basic_operation();
	
	public boolean ReachabilityQuery(int start_id, Rectangle rect)
	{
		VisitedVertices.add(start_id);
		
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