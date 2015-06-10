package def;

import java.util.*;
import java.io.*;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

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
		index.VisitedNodes = new HashSet();
		index.TraversalInEdgeNodes(41311);
		System.out.println(index.VisitedNodes);
	}
	
}