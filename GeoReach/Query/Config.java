package GeoReach;

public class Config	{
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";
	
	private String longitude_property_name = "longitude";
	private String latitude_property_name = "latitude";
	
	private String RMBR_minx_name = "RMBR_minx";
	private String RMBR_miny_name = "RMBR_miny";
	private String RMBR_maxx_name = "RMBR_maxx";
	private String RMBR_maxy_name = "RMBR_maxy";
	
	private String GeoB_name = "GeoB";
	private String bitmap_name = "Bitmap";
	private int merge_ratio = 2;
	private double Max_RMBR_ratio = 0.4;
	private int Max_ReachGrid = 200;
	
	public String GetServerRoot() {
		return SERVER_ROOT_URI;
	}
	
	public String GetLongitudePropertyName() {
		return longitude_property_name;
	}
	
	public String GetLatitudePropertyName() {
		return latitude_property_name;
	}
	
	public String GetRMBR_minx_name()
	{
		return RMBR_minx_name;
	}
	
	public String GetRMBR_miny_name()
	{
		return RMBR_miny_name;
	}
	
	public String GetRMBR_maxx_name()
	{
		return RMBR_maxx_name;
	}
	
	public String GetRMBR_maxy_name()
	{
		return RMBR_maxy_name;
	}
	
	public String GetGeoB_name()
	{
		return GeoB_name;
	}
	
	public String GetBitmap_name()
	{
		return bitmap_name;
	}
	
	public int GetMergeRatio()
	{
		return merge_ratio;
	}
	
	public double GetMax_RMBR_Ratio()
	{
		return Max_RMBR_ratio;
	}
	
	public int GetMax_ReachGrid()
	{
		return Max_ReachGrid;
	}
}