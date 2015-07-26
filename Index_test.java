package def;

import java.util.*;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.*;
import java.net.URI;

public class Index_test	{
	
	public static void main(String[] args) {
		
		Index index = new Index();
		int a = 1/2;
		System.out.println(a);
		//System.out.println(index.GetSpatialPlugin());
		/*Rectangle query_rect = new Rectangle();
		query_rect.min_x = 0;
		query_rect.min_y = 0;
		query_rect.max_x = 5;
		query_rect.max_y = 5;
		System.out.println(index.RangeQueryByRTree("aa", query_rect));*/
		
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
		query_rect.max_x = 900;
		query_rect.max_y = 900;
		HashSet<Integer>ids = index.RangeQueryByRTree("RTree_262144_18_1", query_rect);
		if(ids.contains(176347))
			System.out.println(true);
		*/
		//System.out.println(index.ReachabilityQuery(327914, query_rect,"RTree_262144_18_1","Transitive_closure_262144_18_1"));
		//System.out.println(index.RangeQuery("simplepointlayer", query_rect));
		//System.out.println(index.RangeQueryByRTree("simplepointlayer", query_rect));
		//System.out.println(index.FindSpatialLayer("simplepointlayer"));
		//System.out.println(index.GetSpatialPlugin());
		//index.AddSpatialNodesToPointLayer("simplepointlayer");
		
//		String layername = "DAGRTree_18_1";
		//System.out.println(index.CreatePointLayer(layername));
//		index.AddSpatialNodesToPointLayer(layername);
		
		//Rectangle query_rect = new Rectangle(800,800,900,900);
		//System.out.println(index.ReachabilityQuery(283392, query_rect));
		//System.out.println(index.ReachabilityQuery(125340, query_rect, "RTree_262144_18_1", "Transitive_closure_262144_18_1"));
		//System.out.println(index.ReachabilityQuerySCC(2317412, query_rect, "RTree_18_16", "SCCTransitive_closure_18_16"));
	}
	
}