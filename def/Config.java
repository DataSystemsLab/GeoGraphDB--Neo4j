package def;

public class Config	{
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";
	
	private String longitude_property_name = "longitude_20";
	private String latitude_property_name = "latitude_20";
	
	private String RMBR_minx_name = "test1";
	private String RMBR_miny_name = "test2";
	private String RMBR_maxx_name = "test3";
	private String RMBR_maxy_name = "test4";
	
	private String GeoB_name = "GeoB";
	private String bitmap_name = "MultilevelBitmap_128_2_20";
	private int merge_ratio = 2;
	
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
}