package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Checking_correctness {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		spatial_count();
//		RMBR_count();
		Entity_Checking();
	}
	
	public static void Entity_Checking()
	{
		String datasource = "citeseerx";
		for(int ratio = 20;ratio<=80;ratio+=20)
		{
			String filename = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/Zipf_distributed/"+ratio+"/entity.txt";
			BufferedReader reader1 = null;
			File file1 = null;
			try
			{
				file1 = new File(filename);
				reader1 = new BufferedReader(new FileReader(file1));
				
				String tempString1 = reader1.readLine();
				String[]l = tempString1.split(" ");
				int node_count = Integer.parseInt(l[0]);
				
				int line_count = 0;
				while(((tempString1 = reader1.readLine())!=null))
				{
					line_count++;
				}
				reader1.close();
				if(line_count!=node_count)
					System.out.println(ratio + "is not correct");
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(reader1!=null)
				{
					try
					{
						reader1.close();
					}
					catch(IOException e)
					{	
						e.printStackTrace();
					}
				}
			}
			
		}
	}
	
	public static void RMBR_count()
	{
		int ratio = 20;
		String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/citeseerx/Random_spatial_distributed/"+ratio+"/entity.txt";

		int RMBR_count = 0;
		BufferedReader reader1 = null;
		File file1 = null;
		try
		{
			file1 = new File(filename1);
			reader1 = new BufferedReader(new FileReader(file1));
			
			String tempString1 = reader1.readLine();
			
			while(((tempString1 = reader1.readLine())!=null))
			{
				if(tempString1.endsWith(" "))
					tempString1 = tempString1.substring(0, tempString1.length()-1);

				else
				{
					String s1[]=tempString1.split(" ");
					if(Double.parseDouble(s1[6]) > 0)
						RMBR_count++;					
				}
			}
			reader1.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader1!=null)
			{
				try
				{
					reader1.close();
				}
				catch(IOException e)
				{	
					e.printStackTrace();
				}
			}
		}
		System.out.println(String.format("%d\t%d\n", ratio, RMBR_count));
	}
	
	public static void spatial_count()
	{
		for(int ratio = 20;ratio<=80;ratio+=20)
		{
			String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/citeseerx/Clustered_distributed/"+ratio+"/entity.txt";

			int spa_count = 0;
			BufferedReader reader1 = null;
			File file1 = null;
			try
			{
				file1 = new File(filename1);
				reader1 = new BufferedReader(new FileReader(file1));
				
				String tempString1 = reader1.readLine();
				
				while(((tempString1 = reader1.readLine())!=null))
				{
					if(tempString1.endsWith(" "))
						tempString1 = tempString1.substring(0, tempString1.length()-1);

					else
					{
						String s1[]=tempString1.split(" ");
						if(Integer.parseInt(s1[1]) == 1)
							spa_count++;					
					}
				}
				reader1.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(reader1!=null)
				{
					try
					{
						reader1.close();
					}
					catch(IOException e)
					{	
						e.printStackTrace();
					}
				}
			}
			System.out.println(String.format("%d\t%d\n", ratio, spa_count));
		}
		
		
	}

}
