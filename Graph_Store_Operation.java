package def;

import java.util.*;

class Rectangle	{
	public double min_x;
	public double min_y;
	public double max_x;
	public double max_y;
	
	public Rectangle(double p_min_x, double p_min_y, double p_max_x, double p_max_y)
	{
		min_x = p_min_x;
		min_y = p_min_y;
		max_x = p_max_x;
		max_y = p_max_y;
	}
	
	public Rectangle()
	{
		min_x = 0;
		min_y = 0;
		max_x = 0;
		max_y = 0;
	}
}

public interface Graph_Store_Operation {
	
	ArrayList<Integer> GetAllVertices();
	ArrayList<Integer> GetSpatialVertices();
	String GetVertexAllAttributes(int id);	
	String GetVertexAttributeValue(int id, String attributename);//given a vertex id and name of its attribute, return the attribute value
	ArrayList<Integer> GetOutNeighbors(int id);	
	ArrayList<Integer> GetInNeighbors(int id);//get all in neighbors of a vertex with its given id	
	int GetVertexID(String label, String attribute, String value);//given vertex label and attribute name and value return id of this node(this function requires that this attribute has unique constraint)	
	String AddVertexAttribute(int id, String attributename, String value);//add one attribute to a given id vertex
	boolean IsSpatial(int id);//given a vertex id return a boolean value indicating whether it is a spatial vertex
}