package def;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class Arbitary_usage {

	public static void main(String[] args) {
		String query = null;
		String result = null;
		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
		query = String.format("match (a)-->(b) where id(b) = %d return id(a)",4040605);
		result = p_neo.Execute(query);
		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		for(int j_index = 0;j_index<jsonArr.size();j_index++)
		{
			long id = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsLong();
			OwnMethods.Println(id);
		}
//		MyRectangle rec = null;
//		if(rec == null)
//			OwnMethods.Println(true);
		
//		Config p_con = new Config();
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		GeoReach p_geo = new GeoReach();
//		long graph_size = OwnMethods.GetNodeCount("Patents");
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, 500);
//		Iterator<String> iter = hs.iterator();
//		while(iter.hasNext())
//		{
//			long id = 4*graph_size+Long.parseLong(iter.next());
//			System.out.println(id);
//			String query = String.format("match (n) where id(n) =%d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", id, p_con.GetLongitudePropertyName(), p_con.GetLatitudePropertyName(), p_con.GetRMBR_minx_name(), p_con.GetRMBR_miny_name(), p_con.GetRMBR_maxx_name(), p_con.GetRMBR_maxy_name());
//			String result = p_neo.Execute(query);
//			JsonArray jarr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//			jarr = jarr.get(0).getAsJsonObject().get("row").getAsJsonArray();
//			p_geo.UpdateTraverse(id, jarr);
//		}
		
		
//		Config p_con = new Config();
//		Neo4j_Graph_Store p_neo = new Neo4j_Graph_Store();
//		String query = null;
//		String result = null;
//		query = String.format("match (n)-->(b) where id(b) = %d return n.%s, n.%s, n.%s, n.%s, n.%s, n.%s", 5000000, p_con.GetLongitudePropertyName(), p_con.GetLatitudePropertyName(), p_con.GetRMBR_minx_name(), p_con.GetRMBR_miny_name(), p_con.GetRMBR_maxx_name(), p_con.GetRMBR_maxy_name());
//		result = p_neo.Execute(query);
//		JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//		
//		for(int j_index = 0;j_index<jsonArr.size();j_index++)
//		{
//			JsonArray jArr_start = jsonArr.get(j_index).getAsJsonObject().get("row").getAsJsonArray();
//			System.out.println(jArr_start.toString());
//		}
		
//		GeoReach p_geo = new GeoReach();
//		p_geo.UpdateDeleteEdge(4000000, 5000000);
//		double x = 1.000/3;
//		double y = 1.00/3;
//		System.out.println(x-y);
//		if(x-y == 0)
//			OwnMethods.Println(true);
//		else
//			OwnMethods.Println(false);
//		System.out.println(x);
//		String str = String.format("%.8f", x);
//		System.out.println(str);
		
//		String str = "OjAAAAEAAAAAAI0AEAAAABQAIQAkAC4ANAA8AD8AQABMAE8AWgBhAGUAZwBoAG8AcgB3AHoA/AA8AUQBtQHgAesB+wF7ArMCEwM2A8oDdASgBKQEvgXIBi0HjwiICQwKiQoYDIAMggySDOIMEQ01DWANlA2cDasNGw4FDwwPig+QD4MQlxCFEZsRARIcEoIShxKIEpASABMzFJQVkxcAGDMZABphGpYagBuAHAsdxB2AHgAfCx9ZHwAggyC6IAIjkCOAJAAmACiAKb8pACoALBcsgC3HLgYvgC8DMA42ADgCOIA6iTsAPAA9gD4APwtAEUATQBZAG0AjQCVAL0AxQDVASkJAQ8BDAERARkBHQEjASEBMgEzATcBOBFAKUAxQDlAVUANUIFRQVABV";
////		RoaringBitmap r = new RoaringBitmap();
////		r.add(2);
////		r.add(3);
//		ImmutableRoaringBitmap r = OwnMethods.Deserialize_String_ToRoarBitmap(str);
//		System.out.println(r);
//		System.out.println(r.getCardinality());
		
		//OwnMethods.RestartNeo4jClearCache("Patents");
		
//		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_22m");
//		datasource_a.add("uniprotenc_100m");
//		datasource_a.add("uniprotenc_150m");
//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
//		{
//			String datasource = datasource_a.get(name_index);
//			SpatialIndex.DropTable(datasource, "_zipf");
//		}
		
//		String datasource = "Patents";
//		BatchInserter inserter = null;
//		BufferedReader reader = null;
//		File file = null;
//		Map<String, String> config = new HashMap<String, String>();
//		config.put("dbms.pagecache.memory", "5g");
//		String db_path = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/data/graph.db";
//		int node_count = OwnMethods.GetNodeCount(datasource);
//		int ratio = 60;
//		//{
//			long offset = ratio / 20 * node_count;
//			try
//			{
//				
//			}
//		String datasource = "Patents";
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		double experiment_node_count = 500;
//		String graph_label = "Graph_Random_80";
//		double spatial_total_range = 1000;
//		System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));;
//		HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);
//		ArrayList<String> al = OwnMethods.GenerateStartNode(hs, graph_label);
//		System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//		OwnMethods.GetNodeCount(datasource);
//		
//		Random r = new Random();		
//		double j = 1;
//		while(true)
//		{
//			System.out.println(OwnMethods.ClearCache());
//			System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//			Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + 80);
//			double rect_size = spatial_total_range * Math.sqrt(j/100.0);
//			ArrayList<Double> lx = new ArrayList<Double>();
//			ArrayList<Double> ly = new ArrayList<Double>();
//			
//			int true_count = 0;
//			
//			for(int i = 0;i<al.size();i++)
//			{
//				lx.add(r.nextDouble()*(1000-rect_size));
//				ly.add(r.nextDouble()*(1000-rect_size));
//			}
//			
//			long time = 0;
//			for(int i = 0;i<al.size();i++)
//			{
//				int id = Integer.parseInt(al.get(i));
//				
//				double x = lx.get(i);
//				double y = ly.get(i);
//								
//				MyRectangle rect = new MyRectangle(x,y,x+rect_size,y+rect_size);
//				try 
//				{
//					long start = System.currentTimeMillis();
//					boolean result = spareach.ReachabilityQuery(id, rect);
//					time += (System.currentTimeMillis() - start);
//					System.out.println(i);
//					System.out.println(result);
//					
//					if(result)
//						true_count+=1;
//				} 
//				catch(Exception e)
//				{
//					e.printStackTrace();
//					i = i-1;
//				}		
//				
//			}
//			
//			spareach.Disconnect();
//			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//			
//			double a_time = time/experiment_node_count;
//			
//			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/test.txt", true, j+"\t"+a_time+"\t"+true_count+"\n");
//			if(j<1)
//				j*=10;
//			else
//				j+=10;
//			
//			if(j>60)
//				break;
//			
//			if(a_time>3000)
//				break;
//		}
		
		//for(int i = 0;i<1;i++)
			//System.out.println(i);
		
//		double x = 53.283532305734234;
//		double y = 65.15745003013664;
//		
//		MyRectangle spatialrange = new MyRectangle(0,0,1000,1000);
//		double size = 331.66247903553995;
//		GeoReach_Integrate geo = new GeoReach_Integrate(spatialrange, 5);
//		MyRectangle rect = new MyRectangle(x,y,x+size, y+size);
//		System.out.println(geo.ReachabilityQuery(5219357, rect));
		
	}

}
