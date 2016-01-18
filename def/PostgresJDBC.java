package def;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			System.out.println(e.getCause());
			
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
	
	public static String StartServer()
	{
		String []cmd = {"/bin/bash","-c","echo data | sudo -S sh -c \"/etc/init.d/postgresql start\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String StopServer()
	{
		String []cmd = {"/bin/bash","-c","echo data | sudo -S sh -c \"/etc/init.d/postgresql stop\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}

}



