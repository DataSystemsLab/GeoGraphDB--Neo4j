package def;

import  java.sql. * ;

public class PostgresJDBC {

	public static Connection GetConnection()
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
			System.out.println("here");
			System.out.println(ee.getMessage());
			System.out.println(ee.getCause());
			
        }
		return con;
	}
	
	public static void Close(ResultSet resultSet) 
	{
	 
		if (resultSet == null)
			return;

		if (resultSet != null)
			try {
				resultSet.close();
			} catch (SQLException e) {
				/* Do some exception-logging here. */
				e.printStackTrace();
			}
	}
	
	public static void Close(Statement statement) 
	{

		if (statement == null)
			return;
		
		if (statement != null)
			try {
				statement.close();
			} catch (SQLException e) {
				/* Do some exception-logging here. */
				e.printStackTrace();
			}
	}
	
	public static void Close(Connection con)
	{
		if(con == null)
			return;
		
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}



