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
				String suffix = args[3];
				String filesuffix = args[4];
				for(int i = 0;i<args.length;i++)
					System.out.print(args[i]+"\t");
				System.out.print("\n");
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128, ratio, suffix);
				tt.LoadCompresedBitmap(128, datasource, filesuffix, ratio);
			}
			if(args[0].equals("LoadMultilevelBitmap"))
			{
				System.out.println(args.length);
				MyRectangle rect = new MyRectangle(0,0,1000,1000);
				String datasource = args[1];
				int ratio = Integer.parseInt(args[2]);
				String suffix = args[3];
				String filesuffix = args[4];
				GeoReach_Integrate tt = new GeoReach_Integrate(rect,128, ratio, suffix);
				int merge_count = Integer.parseInt(args[5]);
				for(int i = 0;i<args.length;i++)
					System.out.print(args[i]+"\t");
				System.out.print("\n");
				tt.LoadMultilevelBitmap(128, datasource, filesuffix, ratio, merge_count);
//				tt.Set_HasBitmap_Boolean_Reading(datasource, 128, 200, "Zipf_distributed", ratio);
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
