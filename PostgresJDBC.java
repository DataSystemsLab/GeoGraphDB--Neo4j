package def;

import  java.sql. * ;

public class PostgresJDBC {

	public Connection GetConnection()
	{
		Connection con = null;
		try
		{
			Class.forName( "org.postgresql.Driver" ).newInstance();
			String url = "jdbc:postgresql://localhost:5432/postgres" ;
			con = DriverManager.getConnection(url, "postgres" , "postgres" );   
		}
		catch (Exception ee)
		{
			System.out.println(ee.getMessage());
			System.out.println(ee.getCause());
        }
		return con;
	}
	
}
