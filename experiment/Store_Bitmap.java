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
	
	public static void ReachGridFullToPartialBitmap()
	{
		String datasource = "Patents";
		BufferedReader reader = null;
		File file = null;
		FileWriter fw = null;
		int node_count = 0;
		
		int split_pieces = 128;
		
//		for(int ratio = 20;ratio<=80;ratio+=20)
		int ratio = 80;
		{
			try
			{
				file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_sort.txt");
				reader = new BufferedReader(new FileReader(file));
				
				String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/Bitmap_"+ratio+"_partial.txt";
				
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
	
	public static void ReachGridPartialToBitmap(String datasource)
	{
		BufferedReader reader = null;
		File file = null;
		FileWriter fw = null;
		int node_count = 0;		
		int split_pieces = 128;
	
		for(int ratio = 20;ratio<=80;ratio+=20)
//		int ratio = 80;
		{
			try
			{
				file = new File("D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+"_newpartial.txt");
				reader = new BufferedReader(new FileReader(file));
				
				String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/Bitmap_"+ratio+"_newpartial.txt";
				
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
	
	public static void ReachGridFullToReachGridPartial()
	{
		String datasource = "Patents";
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
	
	public static boolean CompareReachGrid()
	{
		String filename1 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Patents/GeoReachGrid_128/GeoReachGrid_40_partial.txt";
		String filename2 = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Patents/GeoReachGrid_128/GeoReachGrid_40_newpartial.txt";
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
			
			int line = 1;
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
					System.out.println(line);
					System.out.println(tempString1);
					System.out.println(tempString2);
					return false;
				}
			}
			reader1.close();
			reader2.close();
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

	public static void CalculateStorageOverhead()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_22m");
		datasource_a.add("uniprotenc_100m");
		datasource_a.add("uniprotenc_150m");
		int split_pieces = 128;
		String wfilepath = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/Experiment_result/Random/storage.csv";
		for(int i = 0;i<datasource_a.size();i++)
		{
			String datasource = datasource_a.get(i);
			OwnMethods.WriteFile(wfilepath, true, datasource+"\nratio\tFullGrid\tPartialGrid");
			for(int ratio = 20;ratio<=80;ratio+=20)
			{
				BufferedReader reader = null;
				File filefullgrids = null;
				File filepartialgrids = null;
				try
				{
					String readfullgrids = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/Bitmap_"+ratio+".txt";
					filefullgrids = new File(readfullgrids);
					reader = new BufferedReader(new FileReader(filefullgrids));										
					String tempString = reader.readLine();
					long sizefullgrid = 0, sizepartialgrid = 0;
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
					
					String readfpartialgrids = "D:/Graph_05_13/graph_2015_1_24_mfc/data/Real_Data/" + datasource + "/GeoReachGrid_"+split_pieces+"/Bitmap_"+ratio+"_partial.txt";
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
	
	public static void main(String[] args) 
	{
//		System.out.println(CompareReachGrid());
//		ReachGridFullToPartialBitmap();
		ReachGridPartialToBitmap("Patents");
//		ReachGridFullToReachGridPartial();
	}

}
