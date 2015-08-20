package def;

import java.util.*;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.*;
import java.net.URI;
import  java.sql. * ;

public class test {
	
	public static Set<Integer> VisitedVertices = new HashSet();
	
	public static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();

	public static void main(String[] args) throws SQLException {
		
		//Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		
//		System.out.println("hello world");
		
//		System.out.println(OwnMethods.ClearCache());
		System.out.println(OwnMethods.RestartNeo4jClearCache("Patents"));
		
//		OwnMethods p_own = new OwnMethods();
//		HashSet<String> hs = p_own.GenerateRandomInteger(3774768, 100);
//		ArrayList<String> al = p_own.GenerateStartNode(hs, "Graph_Random_20");
//		MyRectangle rect = new MyRectangle(0,0,30,30);
//		
//		
//		Spatial_Reach_Index p_spareach = new Spatial_Reach_Index("Patents_Random_20");
//		
//		for(int i = 0; i<al.size();i++)
//		{
//			System.out.println(i);
//			int id = Integer.parseInt(al.get(i));
//			System.out.println(id);
//			
//			GeoReach p_georeach = new GeoReach();
//			boolean result1 = p_georeach.ReachabilityQuery(id, rect);			
//			System.out.println(result1);
//			
//			boolean result2 = p_spareach.ReachabilityQuery(id, rect);
//			System.out.println(result2);
//			
//			if(result1!=result2)
//			{
//				System.out.println(id);
//				return;
//			}
//			
//		}
		
//		rect = new MyRectangle(0,0,300,300);
//		for(int i = 0; i<al.size();i++)
//		{
//			System.out.println(i);
//			int id = Integer.parseInt(al.get(i));
//			
//			GeoReach p_georeach = new GeoReach();
//			p_georeach.VisitedVertices.clear();
//			boolean result1 = p_georeach.ReachabilityQuery(id, rect);			
//			System.out.println(result1);
//			
//			boolean result2 = p_spareach.ReachabilityQuery(id, rect);
//			System.out.println(result2);
//			
//			if(result1!=result2)
//			{
//				System.out.println(id);
//				return;
//			}
//			
//		}
		
//		MyRectangle rect = new MyRectangle(0,0,10,10);
//		Spatial_Reach_Index p_spareach = new Spatial_Reach_Index("Patents_Random_20");
//		p_spareach.ReachabilityQuery(15099078, rect);

		//System.out.println(p_neo4j_graph_store.GetVertexAllAttributes(2626168));
//		System.out.println( " this is a test " );
//        try
//        {
//           Class.forName( "org.postgresql.Driver" ).newInstance();
//           String url = "jdbc:postgresql://localhost:5432/postgres" ;
//           Connection con = DriverManager.getConnection(url, "postgres" , "postgres" );
//           Statement st = con.createStatement();
//           //String sql = "select * from test";
//           String sql = "insert into test values (3, '10,10')" ;
//           ResultSet rs = st.executeQuery(sql);
//            while (rs.next())
//            {
//               System.out.println(rs.getInt( 1 ));
//               System.out.println(rs.getString( 2 ));
//           }
//           rs.close();
//           st.close();
//           con.close();
//
//        }
//        catch (Exception ee)
//        {
//           System.out.println(ee.getMessage());
//           System.out.println(ee.getCause());
//       }
   }
	
}
