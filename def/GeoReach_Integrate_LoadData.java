package def;

public class GeoReach_Integrate_LoadData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			if(args[0].equals("LoadCompresedBitmap"))
			{
				MyRectangle rect = new MyRectangle(0,0,1000,1000);
				String datasource = args[1];
				int ratio = Integer.parseInt(args[2]);
				Config p_config = new Config();
				String suffix = p_config.GetSuffix();
				String filesuffix = args[3];
				OwnMethods.PrintArray(args);
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128);
				tt.LoadCompresedBitmap(128, datasource, filesuffix, ratio);
			}
			if(args[0].equals("LoadMultilevelBitmap"))
			{
				MyRectangle rect = new MyRectangle(0,0,1000,1000);
				String datasource = args[1];
				int ratio = Integer.parseInt(args[2]);
				String filesuffix = args[3];
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128);
				int merge_count = Integer.parseInt(args[4]);
				OwnMethods.PrintArray(args);
				tt.LoadMultilevelBitmap(128, datasource, filesuffix, ratio, merge_count);
//				tt.Set_HasBitmap_Boolean_Reading(datasource, 128, 200, "Zipf_distributed", ratio);
			}
			if(args[0].equals("Set_HasBitmap_Boolean_Reading"))
			{
				OwnMethods.PrintArray(args);
				MyRectangle rect = new MyRectangle(0,0,1000,1000);
				String datasource = args[1];
				int ratio = Integer.parseInt(args[2]);
				String filesuffix = args[3];
				int threshold = Integer.parseInt(args[4]);
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128);
				tt.Set_HasBitmap_Boolean_Reading(datasource, 128, threshold, filesuffix, ratio);
			}
			if(args[0].equals("LoadPartialCompresedBitmap"))
			{
				OwnMethods.PrintArray(args);
				MyRectangle rect = new MyRectangle(0,0,1000,1000);
				String datasource = args[1];
				int ratio = Integer.parseInt(args[2]);
				String filesuffix = args[3];
				int threshold = 200;
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128);
				tt.LoadPartialCompresedBitmap(128, datasource, filesuffix, ratio, threshold);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
