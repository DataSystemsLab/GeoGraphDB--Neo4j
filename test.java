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

public class test {
	
	public static Set<Integer> VisitedVertices = new HashSet();
	
	public static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();

	public static void main(String[] args) {

		//basic_operation test = new basic_operation();
		//System.out.println(basic_operation.GetVertexAllAttributes(26242));
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
		
		/*ArrayList l = GetSpatialVertices();
		for(int i = 0;i<l.size();i++)
		{
			String str = String.valueOf(l.get(i));
			System.out.println(str);
		}*/
		
		/*ArrayList<String> mfc_result = new ArrayList<String>();
		File file = new File("C:/Users/ysun138/Google Drive/Graph_05_13/graph_2015_1_24_mfc/data/RMBR/query_result.txt");
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			
			while ((tempString = reader.readLine()) != null) 
			{
				mfc_result.add(tempString);
	            // 显示行号
	            System.out.println("line " + line + ": " + tempString);
	            line++;
	        }
	        reader.close();
		}
		
		catch (IOException e) 
		{
            e.printStackTrace();
        }
		
		finally 
		{
            if (reader != null) 
            {
                try 
                {
                    reader.close();
                } 
                catch (IOException e1) 
                {
                }
            }
		}
		
		for(int i = 0;i<mfc_result.size();i++)
		{
			System.out.println(mfc_result.get(i));
		}
		
		ArrayList<Integer> l = GetAllVertices();
		Rectangle rect = new Rectangle();
		rect.min_x = 0; 
		rect.max_x = 70;
		rect.min_y = 0;
		rect.max_y = 70;
		
		
		for(int i = 0;i < l.size();i++)
		{
			int id = l.get(i);
			String result = null;
			
			VisitedVertices.clear();
			
			if(ReachabilityQuery(id, rect))
				result= "true";
			else
				result = "false";
			if(!result.equals(mfc_result.get(i)))
			{
				System.out.println("Inconsist!!!!!");
				System.out.println(i);				
				break;
			}
			else
				System.out.println("You are right!!!!!");
		}*/
		
		//int id = p_neo4j_graph_store.GetVertexID("Author", "name", "\\\"Mohamed Sarwat\\\"");
		//System.out.println(id);
		/*
		Rectangle rect = new Rectangle();
		rect.min_x = -100;
		rect.max_x = 0;
		rect.min_y = 0;
		rect.max_y = 50;
		
		boolean result = ReachabilityQuery(id, rect);
		System.out.println(result);*/
		
		//double[] location = p_neo4j_graph_store.GetVerticeLocation(26242);
		//System.out.println(location[0]);
		
		//String query = "start node = node:simplepointlayer('bbox:[0.0,180.0,0.0,90.0]') return node";
		//System.out.println(p_neo4j_graph_store.Execute(query));
		//System.out.println(p_neo4j_graph_store.GetInNeighbors(273774));
		//System.out.println(p_neo4j_graph_store.GetVertexAttributeValue(0, "name"));
		//System.out.println(p_neo4j_graph_store.HasProperty(0, "k111k"));
		
		/*double location[] = p_neo4j_graph_store.GetVerticeLocation(280975);
		System.out.println(location[0]);
		System.out.println(location[1]);*/
	}
	
}
