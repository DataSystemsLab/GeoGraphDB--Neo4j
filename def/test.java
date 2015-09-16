package def;

import java.util.*;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import  java.sql. * ;

public class test {
	
	public static Set<Integer> VisitedVertices = new HashSet();
	
	public static Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();

	public static void main(String[] args) throws SQLException, IOException {
		
		RoaringBitmap r3 = RoaringBitmap.bitmapOf();
		for(int i = 0;i<5000;i++)
			r3.add(i*2);
		//System.out.println(r3);
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
        System.out.println(serializedstring.getBytes().length);
        System.out.println(serializedstring);
		
		System.out.println(r3.serializedSizeInBytes());
		
//		String datasource = "Patents";
//		int split_pieces = 128;
//		BufferedReader reader = null;
//		File file = null;
//		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
//		int node_count = OwnMethods.GetNodeCount(datasource);
//		long size = 0,size_bitmap = 0;
//		
////		for(int ratio = 40;ratio<100;ratio+=20)
//		int ratio = 80;
//		{
//			long offset = ratio / 20 * node_count;
//			try
//			{
//				file = new File("/home/yuhansun/Documents/Real_data/" + datasource + "/GeoReachGrid_"+split_pieces+"/GeoReachGrid_"+ratio+".txt");
//				reader = new BufferedReader(new FileReader(file));
//				reader.readLine();
//				String tempString = null;
//				while((tempString = reader.readLine())!=null)
//				{
//					if(tempString.endsWith(" "))
//						tempString = tempString.substring(0, tempString.length()-1);
//					String[] l = tempString.split(" ");
//					int id = Integer.parseInt(l[0]);
//					int count = Integer.parseInt(l[1]);
//					if(count == 0)
//						continue;
//					else
//					{
//						RoaringBitmap r3 = new RoaringBitmap();
//						for(int i = 2;i<l.length;i++)
//							r3.add(Integer.parseInt(l[i]));
//						
//						r3.runOptimize();
//						ByteBuffer outbb = ByteBuffer.allocate(r3.serializedSizeInBytes());
//				        // If there were runs of consecutive values, you could
//				        // call mrb.runOptimize(); to improve compression 
//				        r3.serialize(new DataOutputStream(new OutputStream(){
//				            ByteBuffer mBB;
//				            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
//				            public void close() {}
//				            public void flush() {}
//				            public void write(int b) {
//				                mBB.put((byte) b);}
//				            public void write(byte[] b) {mBB.put(b);}            
//				            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
//				        }.init(outbb)));
//				        //
//				        outbb.flip();
//				        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
//				        size+=serializedstring.getBytes().length;
//				        size_bitmap+=r3.getSizeInBytes();
////						System.out.println(serializedstring);
////						ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(serializedstring));
////				        ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
////				        System.out.println("read bitmap "+ irb);
//					}
////					break;
//
//				}
//				reader.close();
//				System.out.println(size);
//				System.out.println(size_bitmap);
//			}
//			catch(IOException e)
//			{
//				e.printStackTrace();
//			}
//			finally
//			{
//				if(reader!=null)
//				{
//					try
//					{
//						reader.close();
//					}
//					catch(IOException e)
//					{					
//					}
//				}
//			}
//		}	
		
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		String query = "match (n) where id(n) = 15099072 return n";
//		JsonObject jo = p_neo.GetVertexAllAttributes(15099072);
//		
//		String bitmap = jo.get("Bitmap_128").getAsString();
//		
//		ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(bitmap));
//        ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
//        System.out.println("read bitmap "+ irb);
		
//		File file = new File("/home/yuhansun/Documents/Real_data/Patents/GeoReachGrid_128/GeoReachGrid_80.txt");
//		try {
//			RoaringBitmap r3 = new RoaringBitmap();
//			BufferedReader reader = new BufferedReader(new FileReader(file));
//			reader.readLine();
//			String tempString = null;
////			FileOutputStream fos = null;
////			DataOutputStream dps = null;
//			long size = 0;
//			while((tempString = reader.readLine())!=null)
//			{
//				if(tempString.endsWith(" "))
//					tempString = tempString.substring(0, tempString.length()-1);
//				String[] l = tempString.split(" ");
//				int id = Integer.parseInt(l[0]);
//				int count = Integer.parseInt(l[1]);
//				if(count == 0)
//					continue;
//				else
//				{
//					for(int i = 2;i<l.length;i++)
//					{
//						r3.add(Integer.parseInt(l[i])+id*128*128-2100000000);
//					}
//					//fos = new FileOutputStream("/home/yuhansun/Documents/Real_data/Patents/GeoReachGrid_128/ser/"+id);
////					dps = new DataOutputStream(fos);
//					
////					r3.serialize(dps);
////					fos.close();
////					dps.close();
//				}
//			}
//			r3.runOptimize();
//			size = r3.serializedSizeInBytes();
//			reader.close();
//			System.out.println(size);
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		//Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		
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
