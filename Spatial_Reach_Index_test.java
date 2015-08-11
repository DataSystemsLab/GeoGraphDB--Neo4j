package def;

public class Spatial_Reach_Index_test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MyRectangle rect = new MyRectangle(0,0,332,332);
		Spatial_Reach_Index spareach = new Spatial_Reach_Index("Patents_Random_20");
		spareach.ReachabilityQuery(6424370, rect);
	}

}
