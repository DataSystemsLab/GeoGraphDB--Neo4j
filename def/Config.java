package def;

public class Config	{
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";
	private String longitude_property_name = "longitude";
	private String latitude_property_name = "latitude";
	
	public String GetServerRoot() {
		return SERVER_ROOT_URI;
	}
	
	public String GetLongitudePropertyName() {
		return longitude_property_name;
	}
	
	public String GetLatitudePropertyName() {
		return latitude_property_name;
	}
}