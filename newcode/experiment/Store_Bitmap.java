package experiment;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.roaringbitmap.RoaringBitmap;

import def.OwnMethods;

public class Store_Bitmap {
	
	public static void ReachGridToBitmapGeneral(String filenae1, String filename2)
	{
		File file = null;
		BufferedReader reader = null;
		FileWriter fw = null;
		try
		{
			file = new File(filenae1);
			reader = new BufferedReader(new FileReader(file));
			
			String tempString = reader.readLine();
			int node_count = Integer.parseInt(tempString);
			
			String writeString = "";
			fw = new FileWriter(filename2,true);
			fw.write(node_count+"\n");
			while((tempString = reader.readLine())!=null)
			{
				if(tempString.endsWith(" "))
					tempString = tempString.substring(0, tempString.length()-1);
				String[] l = tempString.split(" ");
				int id = Integer.parseInt(l[0]);
				int count = Integer.parseInt(l[1]);
				if(count == 0)
				{
					continue;
				}
				else
				{
					RoaringBitmap r3 = new RoaringBitmap();
					for(int i = 2;i<l.length;i++)
						r3.add(Integer.parseInt(l[i]));
					
			        String serializedstring = OwnMethods.Serialize_RoarBitmap_ToString(r3);
					writeString=id+"\t"+serializedstring+"\n";
					fw.write(writeString);					
				}
			}
			reader.close();
			fw.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{	
					e.printStackTrace();
				}
			}
			if(fw!=null)
			{
				try
				{
					fw.close();
				}
				catch(IOException e)
				{	
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void ReachGridFullToBitmapFull(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
		for(int data_index = 0;data_index<datasource_a.size();data_index++)
		{
			String datasource = datasource_a.get(data_index);
			BufferedReader reader = null;
			File file = null;
			FileWriter fw = null;
			int node_count = 0;
			
			int split_pieces = 128;
			
			//for(int ratio = 20;ratio<=80;ratio+=20)
			int ratio = 80;
			{
				try
				{
					file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/GeoReachGrid_"+ratio+".txt");
					reader = new BufferedReader(new FileReader(file));
					
					String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+".txt";
					
					String tempString = reader.readLine();
					node_count = Integer.parseInt(tempString);
					
					String writeString = "";
					fw = new FileWriter(wfilepath,true);
					fw.write(node_count+"\n");
					while((tempString = reader.readLine())!=null)
					{
						if(tempString.endsWith(" "))
							tempString = tempString.substring(0, tempString.length()-1);
						String[] l = tempString.split(" ");
						int id = Integer.parseInt(l[0]);
						int count = Integer.parseInt(l[1]);
						if(count == 0)
						{
							continue;
						}
						else
						{
							RoaringBitmap r3 = new RoaringBitmap();
							for(int i = 2;i<l.length;i++)
								r3.add(Integer.parseInt(l[i]));
							
							r3.runOptimize();
							ByteBuffer outbb = ByteBuffer.allocate(r3.serializedSizeInBytes());
					        // If there were runs of consecutive values, you could
					        // call mrb.runOptimize(); to improve compression 
					        r3.serialize(new DataOutputStream(new OutputStream(){
					            ByteBuffer mBB;
					            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
					            public void close() {}
					            public void flush() {}
					            public void write(int b) {
					                mBB.put((byte) b);}
					            public void write(byte[] b) {mBB.put(b);}            
					            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
					        }.init(outbb)));
					        //
					        outbb.flip();
					        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
							writeString=id+"\t"+serializedstring+"\n";
							fw.write(writeString);					
						}
					}
					reader.close();
					fw.close();
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
						}
						catch(IOException e)
						{	
							e.printStackTrace();
						}
					}
					if(fw!=null)
					{
						try
						{
							fw.close();
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
	
	public static void ReachGridFullToPartialBitmap(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		for(int data_index = 0;data_index<datasource_a.size();data_index++)
		{
			String datasource = datasource_a.get(data_index);
			BufferedReader reader = null;
			File file = null;
			FileWriter fw = null;
			int node_count = 0;
			
			int split_pieces = 128;
			
			for(int ratio = 20;ratio<=80;ratio+=20)
			//int ratio = 80;
			{
				try
				{
					file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/GeoReachGrid_"+ratio+".txt");
					reader = new BufferedReader(new FileReader(file));
					
					String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+"_partial_comparejava.txt";
					
					String tempString = reader.readLine();
					node_count = Integer.parseInt(tempString);
					
					String writeString = "";
					fw = new FileWriter(wfilepath,true);
					fw.write(node_count+"\n");
					while((tempString = reader.readLine())!=null)
					{
						if(tempString.endsWith(" "))
							tempString = tempString.substring(0, tempString.length()-1);
						String[] l = tempString.split(" ");
						int id = Integer.parseInt(l[0]);
						int count = Integer.parseInt(l[1]);
						if(count == 0||count>200)
						{
							continue;
						}
						else
						{
							RoaringBitmap r3 = new RoaringBitmap();
							for(int i = 2;i<l.length;i++)
								r3.add(Integer.parseInt(l[i]));
							
							r3.runOptimize();
							ByteBuffer outbb = ByteBuffer.allocate(r3.serializedSizeInBytes());
					        // If there were runs of consecutive values, you could
					        // call mrb.runOptimize(); to improve compression 
					        r3.serialize(new DataOutputStream(new OutputStream(){
					            ByteBuffer mBB;
					            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
					            public void close() {}
					            public void flush() {}
					            public void write(int b) {
					                mBB.put((byte) b);}
					            public void write(byte[] b) {mBB.put(b);}            
					            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
					        }.init(outbb)));
					        //
					        outbb.flip();
					        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
							writeString=id+"\t"+serializedstring+"\n";
							fw.write(writeString);					
						}
					}
					reader.close();
					fw.close();
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
						}
						catch(IOException e)
						{	
							e.printStackTrace();
						}
					}
					if(fw!=null)
					{
						try
						{
							fw.close();
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
	
	public static void ReachGridPartialToBitmap(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		for(int data_index = 0;data_index<datasource_a.size();data_index++)
		{
			String datasource = datasource_a.get(data_index);
			BufferedReader reader = null;
			File file = null;
			FileWriter fw = null;
			int node_count = 0;		
			int split_pieces = 128;
		
			for(int ratio = 20;ratio<=80;ratio+=20)
//			int ratio = 60;
			{
				try
				{
					file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/GeoReachGrid_"+ratio+"_partial.txt");
					reader = new BufferedReader(new FileReader(file));
					
					String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+"_partial.txt";
					
					String tempString = reader.readLine();
					node_count = Integer.parseInt(tempString);
					
					String writeString = "";
					fw = new FileWriter(wfilepath,true);
					fw.write(node_count+"\n");
					while((tempString = reader.readLine())!=null)
					{
						if(tempString.endsWith(" "))
							tempString = tempString.substring(0, tempString.length()-1);
						String[] l = tempString.split(" ");
						int id = Integer.parseInt(l[0]);
						int count = Integer.parseInt(l[1]);
						RoaringBitmap r3 = new RoaringBitmap();
						for(int i = 2;i<l.length;i++)
							r3.add(Integer.parseInt(l[i]));
						
						r3.runOptimize();
						ByteBuffer outbb = ByteBuffer.allocate(r3.serializedSizeInBytes());
				        // If there were runs of consecutive values, you could
				        // call mrb.runOptimize(); to improve compression 
				        r3.serialize(new DataOutputStream(new OutputStream(){
				            ByteBuffer mBB;
				            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
				            public void close() {}
				            public void flush() {}
				            public void write(int b) {
				                mBB.put((byte) b);}
				            public void write(byte[] b) {mBB.put(b);}            
				            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
				        }.init(outbb)));
				        //
				        outbb.flip();
				        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());

						writeString=id+"\t"+serializedstring+"\n";
						fw.write(writeString);					
					}
					reader.close();
					fw.close();
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
						}
						catch(IOException e)
						{	
							e.printStackTrace();
						}
					}
					if(fw!=null)
					{
						try
						{
							fw.close();
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
	
	public static void ReachGridFullToReachGridPartial()
	{
		String datasource = "citeseerx";
		BufferedReader reader = null;
		File file = null;
		FileWriter fw = null;
		int node_count = 0;
		
		int split_pieces = 128;
		
//		for(int ratio = 20;ratio<=80;ratio+=20)
		int ratio = 40;
		{
			try
			{
				file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_sort.txt");
				reader = new BufferedReader(new FileReader(file));
				
				String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_partial.txt";
				
				String tempString = reader.readLine();
				node_count = Integer.parseInt(tempString);
				
				String writeString = "";
				fw = new FileWriter(wfilepath,true);
				fw.write(node_count+"\n");
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					int count = Integer.parseInt(l[1]);
					if(count == 0||count>200)
					{
						continue;
					}
					else
					{
						fw.write(tempString+"\n");			
					}
				}
				reader.close();
				fw.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(reader!=null)
				{
					try
					{
						reader.close();
					}
					catch(IOException e)
					{	
						e.printStackTrace();
					}
				}
				if(fw!=null)
				{
					try
					{
						fw.close();
					}
					catch(IOException e)
					{	
						e.printStackTrace();
					}
				}
			}
		}	
	}
	
	public static void GeoReachGridMultiLevelToBitmap(String type)
	{
		int split_pieces = 128;
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		for(int data_index = 0;data_index<datasource_a.size();data_index++)
		{
			String datasource = datasource_a.get(data_index);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				for(int mer = 2;mer<=3;mer++)
				{
					String file1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/GeoReachGrid_"+ratio+"_multilevelfull_"+mer+".txt";
					String file2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+"_multilevelfull_"+mer+".txt";
					ReachGridToBitmapGeneral(file1,file2);
				}
			}
		}
	}
	
	public static boolean CompareReachGrid(String filename1, String filename2)
	{
		BufferedReader reader1 = null;
		File file1 = null;
		BufferedReader reader2 = null;
		File file2 = null;
		
		try
		{
			file1 = new File(filename1);
			reader1 = new BufferedReader(new FileReader(file1));
			
			file2 = new File(filename2);
			reader2 = new BufferedReader(new FileReader(file2));
			
			
			String tempString1 = reader1.readLine();
			String tempString2 = reader2.readLine();
			
			int line = 1, diff_count = 0;
			while(((tempString1 = reader1.readLine())!=null)&&((tempString2 = reader2.readLine())!=null))
			{
				line++;
				if(tempString1.endsWith(" "))
					tempString1 = tempString1.substring(0, tempString1.length()-1);
				if(tempString2.endsWith(" "))
					tempString2 = tempString2.substring(0, tempString2.length()-1);
				if(tempString1.equals(tempString2))
					continue;
				else
				{
					String s1[]=tempString1.split(" ");
					String s2[] = tempString2.split(" ");
					int count1 = Integer.parseInt(s1[1]);
					int count2 = Integer.parseInt(s2[1]);
					System.out.println(count1 - count2);
					diff_count+=1;
					System.out.println(line);
					System.out.println(tempString1);
					System.out.println(tempString2);
					return false;
				}
			}
			reader1.close();
			reader2.close();
			System.out.println(line);
			System.out.println(diff_count);
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
			if(reader2!=null)
			{
				try
				{
					reader2.close();
				}
				catch(IOException e)
				{	
					e.printStackTrace();
				}
			}
		}
		
		return true;
	}
	
	public static boolean CompareReachGrid(String filetype, String datasource, int ratio)
	{
		String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_128/"+filetype+"/Bitmap_"+ratio+"_partial.txt";
		String filename2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_128/"+filetype+"/Bitmap_"+ratio+"_partial_comparejava.txt";
		
		if(CompareReachGrid(filename1, filename2))
			return true;
		else
			return false;		
	}

	public static void CalculateStorageOverhead(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		int split_pieces = 128;
		String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Experiment_result/"+type+"/storage.csv";
		for(int i = 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
//			OwnMethods.WriteFile(wfilepath, true, datasource+"\nratio\tFullGrid\tPartialGrid\tmulti-level-2\tmulti-level-3\tmulti-level-4\n");
			OwnMethods.WriteFile(wfilepath, true, datasource+"\nratio\tFullGrid\tPartialGrid\n");
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				BufferedReader reader = null;
				File filefullgrids = null;
				File filepartialgrids = null;
				File filemultilevel = null;
				try
				{
					String readfullgrids = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+".txt";
					filefullgrids = new File(readfullgrids);
					reader = new BufferedReader(new FileReader(filefullgrids));										
					String tempString = reader.readLine();
					long sizefullgrid = 0, sizepartialgrid = 0, sizemultilevel;
					while((tempString = reader.readLine())!=null)
					{
						if(tempString.endsWith(" "))
							tempString = tempString.substring(0, tempString.length()-1);
						String[] l = tempString.split("\t");
						int id = Integer.parseInt(l[0]);
						String bitmap = l[1];
						sizefullgrid+=bitmap.getBytes().length;
					}
					reader.close();
					
					String readfpartialgrids = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+"_partial.txt";
					filepartialgrids = new File(readfpartialgrids);
					reader = new BufferedReader(new FileReader(filepartialgrids));										
					tempString = reader.readLine();
					while((tempString = reader.readLine())!=null)
					{
						if(tempString.endsWith(" "))
							tempString = tempString.substring(0, tempString.length()-1);
						String[] l = tempString.split("\t");
						int id = Integer.parseInt(l[0]);
						String bitmap = l[1];
						sizepartialgrid+=bitmap.getBytes().length;	
					}
					reader.close();
					
					OwnMethods.WriteFile(wfilepath, true, ratio+"\t"+sizefullgrid+"\t"+sizepartialgrid+"\n");

//					sizemultilevel = 0;
//					String readfmultlevel = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_multilevel_2.txt";
//					filemultilevel = new File(readfmultlevel);
//					reader = new BufferedReader(new FileReader(filemultilevel));										
//					tempString = reader.readLine();
//					while((tempString = reader.readLine())!=null)
//					{
//						if(tempString.endsWith(" "))
//							tempString = tempString.substring(0, tempString.length()-1);
//						String[] l = tempString.split("\t");
//						int id = Integer.parseInt(l[0]);
//						String bitmap = l[1];
//						sizemultilevel+=bitmap.getBytes().length;
//					}
//					reader.close();
//					OwnMethods.WriteFile(wfilepath, true, sizemultilevel+"\t");
					
//					sizemultilevel = 0;
//					readfmultlevel = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_multilevel_3.txt";
//					filemultilevel = new File(readfmultlevel);
//					reader = new BufferedReader(new FileReader(filemultilevel));										
//					tempString = reader.readLine();
//					while((tempString = reader.readLine())!=null)
//					{
//						if(tempString.endsWith(" "))
//							tempString = tempString.substring(0, tempString.length()-1);
//						String[] l = tempString.split("\t");
//						int id = Integer.parseInt(l[0]);
//						String bitmap = l[1];
//						sizemultilevel+=bitmap.getBytes().length;
//					}
//					reader.close();
//					OwnMethods.WriteFile(wfilepath, true, sizemultilevel+"\t");
//					
//					sizemultilevel = 0;
//					readfmultlevel = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_multilevel_4.txt";
//					filemultilevel = new File(readfmultlevel);
//					reader = new BufferedReader(new FileReader(filemultilevel));										
//					tempString = reader.readLine();
//					while((tempString = reader.readLine())!=null)
//					{
//						if(tempString.endsWith(" "))
//							tempString = tempString.substring(0, tempString.length()-1);
//						String[] l = tempString.split("\t");
//						int id = Integer.parseInt(l[0]);
//						String bitmap = l[1];
//						sizemultilevel+=bitmap.getBytes().length;
//					}
//					reader.close();
//					OwnMethods.WriteFile(wfilepath, true, sizemultilevel+"\n");
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
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
	
	public static void PartialRMBRCount(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		int split_pieces = 128;
		String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Experiment_result/"+type+"/PartialRMBR_count.csv";
		for(int index = 0;index<datasource_a.size();index++)
		{
			String datasource = datasource_a.get(index);
			OwnMethods.WriteFile(wfilepath, true, datasource+"\n");
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				String rfilepath1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+"_partial.txt";
				String rfilepath2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_"+split_pieces+"/"+type+"/Bitmap_"+ratio+".txt";
				
				BufferedReader reader = null;
				try
				{					
					int total_count = 0;
					int partial_count = 0;
					reader = new BufferedReader(new FileReader(rfilepath1));					

					String str = reader.readLine();
					while((str = reader.readLine())!=null)
						partial_count++;
					reader.close();
					
					reader = new BufferedReader(new FileReader(rfilepath2));
					str = reader.readLine();
					while((str = reader.readLine())!=null)
						total_count++;
					reader.close();
					
					OwnMethods.WriteFile(wfilepath, true, String.format("%d\t%d\t%d\t%d\n", ratio,total_count, partial_count, total_count - partial_count));
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
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
	
	public static void RMBR_size(String type)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		int split_pieces = 128;
		String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Experiment_result/"+type+"/RMBR_size.csv";
		for(int index = 0;index<datasource_a.size();index++)
		{
			String datasource = datasource_a.get(index);
			OwnMethods.WriteFile(wfilepath, true, datasource+"\n");
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				String rfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/"+type+"/"+ratio+"/entity.txt";
				
				BufferedReader reader = null;
				try
				{					
					int RMBR_count = 0;
					reader = new BufferedReader(new FileReader(rfilepath));					

					String str = reader.readLine();
					while((str = reader.readLine())!=null)
					{
						String[] l = str.split(" ");
						if(Double.parseDouble(l[6])>0)
							RMBR_count++;
					}
					reader.close();
					
					OwnMethods.WriteFile(wfilepath, true, String.format("%d\t%d\n", ratio, RMBR_count*4*8));
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(reader!=null)
					{
						try
						{
							reader.close();
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
	
	public static void CompareReachGridFull(String filetype)
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
		for(int i = 0;i<datasource_a.size();i++)
		{
			String str = datasource_a.get(i);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				for(int merge_count = 2;merge_count<=4;merge_count++)
				{
					String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+str+"/GeoReachGrid_128/"+filetype+"/GeoReachGrid_"+ratio+"_multilevelfull_"+merge_count+".txt";
					String filename2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+str+"/GeoReachGrid_128/"+filetype+"/GeoReachGrid_"+ratio+"_multilevelfull_"+merge_count+"_inset.txt";
					CompareReachGrid(filename1, filename2);
				}
			}
		}
	}
	
	public static void main(String[] args) 
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		//datasource_a.add("citeseerx");
		//datasource_a.add("go_uniprot");
		//datasource_a.add("Patents");
		//datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
//		RMBR_size("Clustered_distributed");
//		RMBR_size("Zipf_distributed");
//		PartialRMBRCount("Zipf_distributed");
//		CalculateStorageOverhead("Zipf_distributed");
//		CalculateStorageOverhead("Clustered_distributed");
//		for(int ratio = 20; ratio<=80;ratio+=20)
//		{
//			System.out.println(CompareReachGrid("Zipf_distributed", "go_uniprot", ratio));
//			System.out.println(CompareReachGrid("Zipf_distributed", "uniprotenc_22m", ratio));
//			
//			System.out.println(CompareReachGrid("Zipf_distributed", "citeseerx", ratio));
//			System.out.println(CompareReachGrid("Zipf_distributed", "Patents", ratio));
//			System.out.println(CompareReachGrid("Zipf_distributed", "uniprotenc_100m", ratio));
//			System.out.println(CompareReachGrid("Zipf_distributed", "uniprotenc_150m", ratio));
//			
//		}
//		ReachGridFullToPartialBitmap("Zipf_distributed");
//		ReachGridFullToBitmapFull("Zipf_distributed");
//		ReachGridPartialToBitmap("Zipf_distributed");
		
		
//		ReachGridPartialToBitmap("Patents");
//		ReachGridPartialToBitmap("uniprotenc_22m");
//		ReachGridPartialToBitmap("uniprotenc_100m");
//		ReachGridPartialToBitmap("uniprotenc_150m");
//		ReachGridFullToReachGridPartial();
		
		//ReachGridFullToBitmapFull("Random_spatial_distributed");
		//ReachGridFullToBitmapFull("Clustered_distributed");
//		ReachGridFullToBitmapFull("Zipf_distributed");
		
//		ReachGridPartialToBitmap("Clustered_distributed");
		
//		PartialRMBRCount("Clustered_distributed");
//		PartialRMBRCount("Zipf_distributed");
		
//		for(int ratio = 20;ratio<=80;ratio+=20)
//		{
//			for(int mer = 2;mer<=3;mer++)
//			{
//				String file1="D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/uniprotenc_22m/GeoReachGrid_128/Zipf_distributed/GeoReachGrid_"+ratio+"_multilevelfull_"+mer+".txt";
//				String file2="D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/uniprotenc_22m/GeoReachGrid_128/Zipf_distributed/GeoReachGrid_"+ratio+"_multilevelfull_"+mer+"_inset.txt";
//				System.out.println(CompareReachGrid(file1, file2));
//			}
//		}
		
		/*for(int i = 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				for(int mergecount = 2;mergecount<=4;mergecount++)
				{
					String file1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_128/Random_spatial_distributed/GeoReachGrid_20_multilevel_"+mergecount+".txt";
					String file2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/"+datasource+"/GeoReachGrid_128/Random_spatial_distributed/GeoReachGrid_20_multilevel_"+mergecount+"_inset.txt";
					System.out.println(CompareReachGrid(file1, file2));
				}
			}
		}*/
		
//		ReachGridToBitmapGeneral("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/citeseerx/GeoReachGrid_128/Zipf_distributed/GeoReachGrid_80_multilevelfull_4_new.txt","D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/citeseerx/GeoReachGrid_128/Zipf_distributed/Bitmap_80_multilevelfull_4.txt");
//		CompareReachGridFull("Clustered_distributed");
		
		GeoReachGridMultiLevelToBitmap("Random_spatial_distributed");
	}

}
