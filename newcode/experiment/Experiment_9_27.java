package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import def.GeoReach;
import def.GeoReach_Integrate;
import def.MyRectangle;
import def.Neo4j_Graph_Store;
import def.OwnMethods;
import def.PostgresJDBC;
import def.Spatial_Reach_Index;

public class Experiment_9_27 {
	
	public static ArrayList<Long> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt";
		int offset = OwnMethods.GetNodeCount(datasource);
		ArrayList<Long> al = new ArrayList<Long>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(Long.parseLong(temp)+offset);
			}
			reader.close();
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
				}
			}
		}
		return al;
	}

	public static void main(String[] args) 
	{
		if(args[0].equals("GenerateExperimentNode"))
		{
			String datasource = args[1];
			int experiment_node_count = 500;
			long graph_size = OwnMethods.GetNodeCount(datasource);
			HashSet<String> hs = OwnMethods.GenerateRandomInteger(graph_size, (int)experiment_node_count);						
			Iterator<String> iter = hs.iterator();
			while(iter.hasNext())
			{
				long id = Integer.parseInt(iter.next());
				OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt", true, id+"\n");
			}
		}
		else
		{
			String suffix = args[1];
			//String suffix = "_random";
//			ArrayList<String> datasource_a = new ArrayList<String>();
			//datasource_a.add("citeseerx");
			//datasource_a.add("go_uniprot");
//			datasource_a.add("Patents");
//			datasource_a.add("uniprotenc_22m");
//			datasource_a.add("uniprotenc_100m");
//			datasource_a.add("uniprotenc_150m");
//			for(int name_index = 0;name_index<datasource_a.size();name_index++)
			{
//				String datasource = datasource_a.get(name_index);
				String datasource = args[0];
				String resultpath = "/home/yuhansun/Documents/Real_data/query_time_9_27"+suffix+"_full_multi.csv";
				OwnMethods.WriteFile(resultpath, true, datasource+"\n");
				int pieces = 128;
				{
					boolean isbreak = false;
					long graph_size = OwnMethods.GetNodeCount(datasource);
					long experiment_node_count = 500;
					
					ArrayList<Long> al = null;
					al = ReadExperimentNode(datasource);
					

					for(int ratio = 20;ratio<=80;ratio+=20)
					{
						OwnMethods.WriteFile(resultpath, true, ratio+"\n");
						OwnMethods.WriteFile(resultpath, true, "spatial_range\tGeoReach_RMBR\tGeoReach_Full\tGeoReach_Multi_2\tGeoReach_Multi_3\tGeoReach_Partial\tSpaReach\ttrue_count\n");
						String graph_label = "Graph_Random_" + ratio;
						
						
						ArrayList<Double> a_x = new ArrayList<Double>();
						ArrayList<Double> a_y = new ArrayList<Double>();
						
						Random r = new Random();
						
						double selectivity = 0.0001;
						double spatial_total_range = 1000;
						
						boolean isrun = true;
						{
							while(selectivity<=1)
							{
								double rect_size = spatial_total_range * Math.sqrt(selectivity);
								OwnMethods.WriteFile(resultpath, true, selectivity+"\t");
								
								a_x.clear();
								a_y.clear();
								for(int i = 0;i<experiment_node_count;i++)
								{
									a_x.add(r.nextDouble()*(1000-rect_size));
									a_y.add(r.nextDouble()*(1000-rect_size));
								}
								
								int true_count = 0;
								ArrayList<Boolean> geo_RMBR_result = new ArrayList<Boolean>();
								ArrayList<Boolean> geo_full_result = new ArrayList<Boolean>();
								ArrayList<Boolean> geo_multilevel2_result = new ArrayList<Boolean>();
								ArrayList<Boolean> geo_multilevel3_result = new ArrayList<Boolean>();
								ArrayList<Boolean> geo_partial_result = new ArrayList<Boolean>();
								ArrayList<Boolean> spareach_result = new ArrayList<Boolean>();
								{
									//GeoReach_RMBR
									System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									GeoReach georeach_RMBR = new GeoReach(suffix, ratio);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									int accessnodecount = 0, time_georeach_RMBR = 0;
									for(int i = 0;i<al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										System.out.println(i);
										long id = al.get(i);
										System.out.println(id);
										
										try
										{
											georeach_RMBR.VisitedVertices.clear();
											long start = System.currentTimeMillis();
											boolean result3 = georeach_RMBR.ReachabilityQuery(id, query_rect);
											time_georeach_RMBR += System.currentTimeMillis() - start;
											System.out.println(result3);
											geo_RMBR_result.add(result3);
											accessnodecount+=georeach_RMBR.VisitedVertices.size();
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									OwnMethods.WriteFile(resultpath, true, time_georeach_RMBR/experiment_node_count+"\t");
									
									//GeoReach_Full
									System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									MyRectangle rect = new MyRectangle(0,0,1000,1000);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									GeoReach_Integrate georeach_full = new GeoReach_Integrate(rect, pieces, ratio, suffix);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									accessnodecount = 0; 
									int time_georeach_full = 0;
									for(int i = 0;i<al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										System.out.println(i);
										long id = al.get(i);
										System.out.println(id);
										
										try
										{
											georeach_full.VisitedVertices.clear();
											long start = System.currentTimeMillis();
											boolean result3 = georeach_full.ReachabilityQuery_FullGrids(id, query_rect);
											time_georeach_full += System.currentTimeMillis() - start;
											System.out.println(result3);
											geo_full_result.add(result3);
											accessnodecount+=georeach_full.VisitedVertices.size();
											if(pieces == 128)
												if(result3)
													true_count+=1;
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									OwnMethods.WriteFile(resultpath, true, time_georeach_full/experiment_node_count+"\t");
									
									//GeoReach_Multilevel2
									System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									GeoReach_Integrate georeach_multilevel2 = new GeoReach_Integrate(rect, pieces, ratio, suffix);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									accessnodecount = 0; 
									int time_georeach_multilevel2 = 0;
									for(int i = 0;i<al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										System.out.println(i);
										long id = al.get(i);
										System.out.println(id);
										
										try
										{
											georeach_multilevel2.VisitedVertices.clear();
											long start = System.currentTimeMillis();
											boolean result = georeach_multilevel2.ReachabilityQuery_Bitmap_MultiResolution(id, query_rect,2);
											time_georeach_multilevel2 += System.currentTimeMillis() - start;
											System.out.println(result);
											geo_multilevel2_result.add(result);
											accessnodecount+=georeach_multilevel2.VisitedVertices.size();
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									OwnMethods.WriteFile(resultpath, true, time_georeach_multilevel2/experiment_node_count+"\t");
									
									//GeoReach_Multilevel3
									System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									GeoReach_Integrate georeach_multilevel3 = new GeoReach_Integrate(rect, pieces, ratio, suffix);
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									accessnodecount = 0; 
									int time_georeach_multilevel3 = 0;
									for(int i = 0;i<al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										System.out.println(i);
										long id = al.get(i);
										System.out.println(id);
										
										try
										{
											georeach_multilevel3.VisitedVertices.clear();
											long start = System.currentTimeMillis();
											boolean result = georeach_multilevel3.ReachabilityQuery_Bitmap_MultiResolution(id, query_rect,3);
											time_georeach_multilevel3 += System.currentTimeMillis() - start;
											System.out.println(result);
											geo_multilevel3_result.add(result);
											accessnodecount+=georeach_multilevel3.VisitedVertices.size();
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									OwnMethods.WriteFile(resultpath, true, time_georeach_multilevel3/experiment_node_count+"\t");
									
									//GeoReach_Partial
									System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
									System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
									
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									
									GeoReach_Integrate georeach_partial = new GeoReach_Integrate(rect, pieces, ratio, suffix);
									
									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									
									accessnodecount = 0;
									int time_georeach_partial = 0;
									for(int i = 0;i<al.size();i++)
									{
										double x = a_x.get(i);
										double y = a_y.get(i);
										MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
										
										System.out.println(i);
										long id = (al.get(i));
										System.out.println(id);
										
										try
										{
											georeach_partial.VisitedVertices.clear();
											long start = System.currentTimeMillis();
											boolean result3 = georeach_partial.ReachabilityQuery_Bitmap_Partial(id, query_rect);
											time_georeach_partial += System.currentTimeMillis() - start;
											System.out.println(result3);
											geo_partial_result.add(result3);
											accessnodecount+=georeach_partial.VisitedVertices.size();
										}
										catch(Exception e)
										{
											e.printStackTrace();
											OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
											i = i-1;
										}						
									}
									OwnMethods.WriteFile(resultpath, true, time_georeach_partial/experiment_node_count+"\t");
									
									//SpaReach
//									if(isrun)
//									{
//										int time_spareach = 0;
//										System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
//										System.out.println(PostgresJDBC.StopServer());
//										System.out.println(OwnMethods.ClearCache());
//										System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//										System.out.println(PostgresJDBC.StartServer());
//										System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
//										try {
//											Thread.currentThread().sleep(5000);
//										} catch (InterruptedException e1) {
//											// TODO Auto-generated catch block
//											e1.printStackTrace();
//										}
//										Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio);
//										try {
//											Thread.currentThread().sleep(5000);
//										} catch (InterruptedException e1) {
//											// TODO Auto-generated catch block
//											e1.printStackTrace();
//										}
//										for(int i = 0;i<al.size();i++)
//										{
//											double x = a_x.get(i);
//											double y = a_y.get(i);
//											MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);
//											
//											System.out.println(i);
//											int id = Integer.parseInt(al.get(i));
//											System.out.println(id);
//											
//											try
//											{
//												long start = System.currentTimeMillis();
//												boolean result2 = spareach.ReachabilityQuery(id, query_rect);
//												time_spareach += (System.currentTimeMillis() - start);
//												System.out.println(result2);
//												spareach_result.add(result2);
//											}
//											catch(Exception e)
//											{
//												e.printStackTrace();
//												OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
//												i = i-1;
//											}						
//										}
//										spareach.Disconnect();
//										if(time_spareach/experiment_node_count>10000)
//											isrun = false;
//										OwnMethods.WriteFile(resultpath, true, time_spareach/experiment_node_count+"\t");
//									}
//									else
										OwnMethods.WriteFile(resultpath, true, "\t");
									
								}
								selectivity*=10;
								OwnMethods.WriteFile(resultpath, true, true_count+"\n");
								for(int i = 0;i<experiment_node_count;i++)
								{
//									if(geo_RMBR_result.get(i)!=geo_full_result.get(i)||geo_RMBR_result.get(i)!=geo_partial_result.get(i)||geo_RMBR_result.get(i)!=spareach_result.get(i))
									//if(geo_full_result.get(i)!=geo_multilevel2_result.get(i)||geo_full_result.get(i)!=geo_multilevel3_result.get(i))
									if(geo_RMBR_result.get(i)!=geo_full_result.get(i)||geo_RMBR_result.get(i)!=geo_partial_result.get(i)||geo_RMBR_result.get(i)!=geo_multilevel2_result.get(i)||geo_RMBR_result.get(i)!=geo_multilevel3_result.get(i))
									{
										System.out.println(al.get(i));
										System.out.println(a_x.get(i));
										System.out.println(a_y.get(i));
										System.out.println(rect_size);
										System.out.println(geo_RMBR_result.get(i));
										System.out.println(geo_full_result.get(i));
										System.out.println(geo_partial_result.get(i));
										System.out.println(geo_multilevel2_result.get(i));
										System.out.println(geo_multilevel3_result.get(i));
										isbreak = true;
										String break_log = "/home/yuhansun/Documents/Real_data/break"+suffix+".log";
										OwnMethods.WriteFile(break_log, true, ""+al.get(i)+"\t"+a_x.get(i)+"\t"+a_y.get(i)+"\t"+rect_size+"\n");
										break;
									}
								}
								if(isbreak)
									break;
							}
						}
						OwnMethods.WriteFile(resultpath, true, "\n");
						if(isbreak)
							break;
					}
				}
				
				
				OwnMethods.WriteFile(resultpath, true, "\n");
				System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
			}		
		}
		
	}

}
