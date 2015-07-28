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

	public static void main(String[] args) {

		//System.out.println(p_neo4j_graph_store.GetVertexAllAttributes(2626168));
		System.out.println( " this is a test " );
        try
        {
           Class.forName( "org.postgresql.Driver" ).newInstance();
           String url = "jdbc:postgresql://localhost:5432/postgres" ;
           Connection con = DriverManager.getConnection(url, "postgres" , "postgres" );
           Statement st = con.createStatement();
           String sql = "select * from test " ;
           ResultSet rs = st.executeQuery(sql);
            while (rs.next())
            {
               System.out.print(rs.getInt( 1 ));
               System.out.println(rs.getString( 2 ));
           }
           rs.close();
           st.close();
           con.close();

}
        catch (Exception ee)
        {
           System.out.println(ee.getMessage());
           System.out.println(ee.getCause());
       }
   }
	
}
