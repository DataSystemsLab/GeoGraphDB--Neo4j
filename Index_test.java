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
		
		/*
		String x = "http://localhost:7474/db/data/ext/SpatialPlugin";
		WebResource resource = Client.create().resource(x);
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		//ClientResponse response = resource.get(ClientResponse.class);
		String result = response.getEntity(String.class);
		System.out.println(result);*/
		
		/*
		String SERVER_ROOT_URI="http://localhost:7474/db/data/";
		final String spatial_add_node = SERVER_ROOT_URI + "ext/SpatialPlugin/graphdb/addGeometryWKTToLayer";
		WebResource resource = Client.create().resource(spatial_add_node);
		
		
		String entity = " { \"layer\": \"geom\", \n \"geometry\" : \"POINT(10 10)\" } ";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).post(ClientResponse.class);
		System.out.println(response.getEntity(String.class));
		//System.out.println(response.getStatus());
		response.close();*/
		
		Index index = new Index();
		//System.out.println(index.RangeQuery("simplepointlayer"));
		//index.ConstructWKTRTree();
		//index.CreatePointLayer();
		//index.CreateSpatialIndex();
		//index.CreatePoints();
		//index.AddPointsToIndex();
		//index.CreateTransitiveClosure();
		//index.VisitedNodes = new HashSet();
		//index.TraversalInReachNodes(41311);
		//System.out.println(index.VisitedNodes);
		/*long start = System.currentTimeMillis();
		index.CreateTransitiveClosureFromInNeighbor();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		/*long start = System.currentTimeMillis();
		index.CreateTransitiveClosureFromInNeighbor();
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);*/
		
		/*String SERVER_ROOT_URI="http://localhost:7474/db/data";
		final String legacy = SERVER_ROOT_URI + "/index/node/";
		WebResource resource = Client.create().resource(legacy);
		
		//String entity = "{\"name\" : \"simplepointlayer\"}";
		
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		System.out.println(response.getEntity(String.class));*/
		//System.out.println(response.getStatus());
		//System.out.println(index.FindSpatialLayer("geom"));
		
		Rectangle query_rect = new Rectangle();
		query_rect.min_x = -180;
		query_rect.min_y = 0;
		query_rect.max_x = 0;
		query_rect.max_y = 90;
		System.out.println(index.RangeQuery("simplepointlayer", query_rect));
	}
	
}