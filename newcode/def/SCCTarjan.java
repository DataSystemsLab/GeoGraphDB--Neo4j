package def;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SCCTarjan {

	List<Integer>[] graph;
	  boolean[] visited;
	  Stack<Integer> stack;
	  int time;
	  int[] lowlink;
	  List<List<Integer>> components;

	  public List<List<Integer>> scc(List<Integer>[] graph) {
	    int n = graph.length;
	    this.graph = graph;
	    visited = new boolean[n];
	    stack = new Stack<Integer>();
	    time = 0;
	    lowlink = new int[n];
	    components = new ArrayList<List<Integer>>();

	    for (int u = 0; u < n; u++)
	      if (!visited[u])
	        dfs(u);

	    return components;
	  }

	  void dfs(int u) {
	    lowlink[u] = time++;
	    visited[u] = true;
	    stack.add(u);
	    boolean isComponentRoot = true;

	    for (int v : graph[u]) {
	      if (!visited[v])
	        dfs(v);
	      if (lowlink[u] > lowlink[v]) {
	        lowlink[u] = lowlink[v];
	        isComponentRoot = false;
	      }
	    }

	    if (isComponentRoot) {
	      List<Integer> component = new ArrayList<Integer>();
	      while (true) {
	        int x = stack.pop();
	        component.add(x);
	        lowlink[x] = Integer.MAX_VALUE;
	        if (x == u)
	          break;
	      }
	      components.add(component);
	    }
	  }
	  
	// Usage example
	  public static void main(String[] args) {	  
		  
		  List<Integer>[] g = null;
		  String path = "C:\\Users\\ysun138\\Google Drive\\Graph_05_13\\graph_2015_1_24_mfc\\data\\RMBR\\18_16\\";
		  File file = new File(path + "graph_entity.txt");
			BufferedReader reader = null;
			
			try
			{
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				int node_count = Integer.parseInt(tempString);
				
				g = new List[node_count];
				  for (int i = 0; i < g.length; i++)
				      g[i] = new ArrayList<Integer>();
				 
				int i = 0;
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					if(l[1].equals('0'))
					{
						i++;
						continue;
					}
					for(int j = 2;j<l.length;j++)
					{
						int id = Integer.parseInt(l[j]);
						g[i].add(id);
					}
					System.out.println(g[i]);
					i++;
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

	    List<List<Integer>> components = new SCCTarjan().scc(g);
	    
	    try 
		{
			FileWriter fw = new FileWriter(path + "SCC.txt",false);
			fw.write(components.size() + "\n");
			for(int i = 0;i<components.size();i++)
			{
				fw.write("[");
				int size = components.get(i).size();
				for(int j = 0;j<size - 1;j++)
					fw.write(components.get(i).get(j).toString()+", ");
				fw.write(components.get(i).get(size - 1).toString() + "]\n");
			}
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	    

	  }
}
