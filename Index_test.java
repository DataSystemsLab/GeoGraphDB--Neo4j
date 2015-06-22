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

public class Index_test	{
	
	public static void main(String[] args) {
		
		Index index = new Index();

		
		/*long start = System.currentTimeMillis();
		index.CreateTransitiveClosureFromInNeighbor();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		
		/*long start = System.currentTimeMillis();
		index.CreateTransitiveClosureFromInNeighbor();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		
		
		/*Rectangle query_rect = new Rectangle();
		query_rect.min_x = 200;
		query_rect.min_y = 200;
		query_rect.max_x = 300;
		query_rect.max_y = 300;
		System.out.println(index.RangeQueryByRTree("simplepointlayer", query_rect));*/
		//System.out.println(index.FindSpatialLayer("simplepointlayer"));
		//System.out.println(index.GetSpatialPlugin());
		//index.AddSpatialNodesToPointLayer("simplepointlayer");
		
		String layername = "RTree_65536_16_1";
		//index.CreatePointLayer(layername);
		index.AddSpatialNodesToPointLayer(layername);
		
		//Rectangle query_rect = new Rectangle(0,0,300,300);
		//System.out.println(index.ReachabilityQuery(283392, query_rect));
	}
	
}