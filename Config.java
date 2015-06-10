package def;

import java.util.*;

public class Config	{
	private String SERVER_ROOT_URI;
	private String longitude_property_name;
	private String latitude_property_name;
	
	public String GetServerRoot() {
		return "http://localhost:7474/db/data";
	}
	
	public String GetLongitudePropertyName() {
		return "longitude";
	}
	
	public String GetLatitudePropertyName() {
		return "latitude";
	}
}