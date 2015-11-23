package def;

import java.util.*;

import com.google.gson.JsonObject;

public interface Graph_Store_Operation {
	
	ArrayList<Integer> GetAllVertices();
	ArrayList<Integer> GetSpatialVertices();
	JsonObject GetVertexAllAttributes(long id);	
	String GetVertexAttributeValue(int id, String attributename);//given a vertex id and name of its attribute, return the attribute value
	ArrayList<Integer> GetOutNeighbors(int id);	
	ArrayList<Integer> GetInNeighbors(int id);//get all in neighbors of a vertex with its given id	
	int GetVertexID(String label, String attribute, String value);//given vertex label and attribute name and value return id of this node(this function requires that this attribute has unique constraint)	
	String AddVertexAttribute(int id, String attributename, String value);//add one attribute to a given id vertex
	boolean IsSpatial(int id);//given a vertex id return a boolean value indicating whether it is a spatial vertex
}