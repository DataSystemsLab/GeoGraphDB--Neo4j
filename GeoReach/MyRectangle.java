package GeoReach;

public class MyRectangle {
	
	public double min_x;
	public double min_y;
	public double max_x;
	public double max_y;
	
	public MyRectangle(double p_min_x, double p_min_y, double p_max_x, double p_max_y)
	{
		min_x = p_min_x;
		min_y = p_min_y;
		max_x = p_max_x;
		max_y = p_max_y;
	}
	
	public MyRectangle()
	{
		min_x = 0;
		min_y = 0;
		max_x = 0;
		max_y = 0;
	}
}
