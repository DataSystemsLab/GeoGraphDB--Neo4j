package experiment;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
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

	public static void main(String[] args) {
		
		String datasource = "Patents";
		BufferedReader reader = null;
		File file = null;
		int node_count = OwnMethods.GetNodeCount(datasource);
		
		int split_pieces = 128;
		int compressed_count = 0, nocompressed_count = 0;
		long compressed_size = 0, nocompressed_size = 0;
		int[] reachgrids_count = new int[128*128];
		long[] reachgrids_size = new long[128*128];
		for(int i = 0;i<128*128;i++)
		{
			reachgrids_count[i] = 0;
			reachgrids_size[i] = 0;
		}
		
		
//		for(int ratio = 40;ratio<100;ratio+=20)
		int ratio = 80;
		{
			try
			{
				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+".txt");
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					if(tempString.endsWith(" "))
						tempString = tempString.substring(0, tempString.length()-1);
					String[] l = tempString.split(" ");
					int id = Integer.parseInt(l[0]);
					int count = Integer.parseInt(l[1]);
					if(count == 0)
						continue;
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
				        reachgrids_count[count]+=1;
				        reachgrids_size[count]+=serializedstring.getBytes().length;
						if(serializedstring.getBytes().length>2048)
						{
							nocompressed_count++;
							nocompressed_size+=serializedstring.getBytes().length;
						}
						else
						{
							compressed_count++;
							compressed_size+=serializedstring.getBytes().length;
						}		        
					}
				}
				reader.close();
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
					}
				}
			}
			System.out.println(compressed_count);
			System.out.println(compressed_size);
			System.out.println(nocompressed_count);
			System.out.println(nocompressed_size);
			for(int i = 0;i<128*128;i++)
			{
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/Patents/a_store.csv", true, i+"\t"+reachgrids_count[i]+"\t"+reachgrids_size[i]+"\t"+(double)reachgrids_size[i]/(double)reachgrids_count[i]+"\n");
			}
		}	
	}

}
