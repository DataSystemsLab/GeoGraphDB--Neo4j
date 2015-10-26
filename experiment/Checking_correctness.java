package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import def.OwnMethods;

public class Checking_correctness {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		spatial_count();
//		RMBR_count();
//		Entity_Ratio_Checking();
		Entity_Location_Checking();
//		RMBR_count("Random_spatial_distributed");
//		RMBR_count("Clustered_distributed");
//		RMBR_count("Zipf_distributed");
		
	}
	
	public static void Entity_Location_Checking()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		for(int ds_index = 0;ds_index<datasource_a.size();ds_index++)
		{
			String datasource = datasource_a.get(ds_index);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				String filename = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/Clustered_distributed/"+ratio+"/entity.txt";
//				String filename = "D:/Clustered_distributed/"+ratio+"/entity.txt";
				
				BufferedReader reader1 = null;
				File file1 = null;
				try
				{
					file1 = new File(filename);
					reader1 = new BufferedReader(new FileReader(file1));
					
					String tempString1 = reader1.readLine();
					String[]l = tempString1.split(" ");
					int node_count = Integer.parseInt(l[0]);
					
					while(((tempString1 = reader1.readLine())!=null))
					{
						l = tempString1.split(" ");
						int id = Integer.parseInt(l[0]);
						int isspatial = Integer.parseInt(l[1]);
						if(isspatial == 1)
						{
							double x = Double.parseDouble(l[2]);
							double y = Double.parseDouble(l[3]);
							if(x<0||x>1000||y<0||y>1000)
							{
								System.out.println(datasource+"\t"+ratio+"\t"+id+"\t"+x+"\t"+y);
								break;
							}
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
				
			}
		}
		
	}
	
	public static void Entity_Ratio_Checking()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		for(int ds_index = 0;ds_index<datasource_a.size();ds_index++)
		{
			String datasource = datasource_a.get(ds_index);
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
					int spa_count = 0;
					while(((tempString1 = reader1.readLine())!=null))
					{
						l = tempString1.split(" ");
						if(Integer.parseInt(l[1]) == 1)
							spa_count += 1;
						line_count++;
					}
					reader1.close();
					System.out.println((int)(node_count*(1-ratio/100.0)) - spa_count);
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
		
	}
	
	public static void RMBR_count(String filetype)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		String wfile = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Experiment_result/"+filetype+"/total_RMBR_size.csv";
		for(int ds_index = 0;ds_index<datasource_a.size();ds_index++)
		{
			String datasource = datasource_a.get(ds_index);
			OwnMethods.WriteFile(wfile, true, datasource+"\n");
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/"+filetype+"/"+ratio+"/entity.txt";
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
					int RMBR_size = RMBR_count*4*8;
					OwnMethods.WriteFile(wfile, true, String.format("%d\t%d\n", ratio, RMBR_size));
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
		}
		
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
