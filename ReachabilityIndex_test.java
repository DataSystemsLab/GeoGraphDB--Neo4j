package def;

public class ReachabilityIndex_test {

	public static void main(String[] args) {
		
		ReachabilityIndex test = new ReachabilityIndex();
		MyRectangle rect = new MyRectangle(0,0,700,700);
		System.out.println(test.ReachabilityQuery(3774768 * 2+1,rect, "", "Graph_Random_20"));
	}

}
