#include "stdafx.h"
#include "Universe.h"

bool Location_In_Rect(Location m_location, MyRect m_rect)
{
	if (m_location.x<m_rect.left_bottom.x || m_location.x>m_rect.right_top.x || m_location.y<m_rect.left_bottom.y || m_location.y>m_rect.right_top.y)
		return false;
	else
		return true;
}

void OutFile(Entity Entity_Matrix[], int node_count, string filename)
{
	ofstream file;
	file.open("data/" + filename);
	for (int i = 0; i < node_count; i++)
	{
		file << i << "    " << Entity_Matrix[i].IsSpatial << "    " << Entity_Matrix[i].location.x << "  " << Entity_Matrix[i].location.y << "    " << Entity_Matrix[i].type;
		file << endl;
	}
	file.close();
}

void OutFile(vector<Entity> &entity_vector, string filename)
{
	ofstream file;
	file.open("data/" + filename);
	for (int i = 0; i < entity_vector.size(); i++)
	{
		file << i << "    " << entity_vector[i].IsSpatial << "    " << entity_vector[i].location.x << "  " << entity_vector[i].location.y << "    " << entity_vector[i].type;
		file << endl;
	}
	file.close();
}

void EntityToDisk(Entity Entity_Matrix[], int node_count, int range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);
	
	printf("%d %d\n", node_count,range);
	for (int i = 0; i < node_count; i++)
	{
		printf("%d %d %f %f %d\n", i, Entity_Matrix[i].IsSpatial, Entity_Matrix[i].location.x, Entity_Matrix[i].location.y, Entity_Matrix[i].type);
	}
	fclose(stdout);
}

void EntityToDisk(vector<Entity> entity_vector, int range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d %d\n", entity_vector.size(), range);
	for (int i = 0; i < entity_vector.size(); i++)
	{
		printf("%d %d %f %f %d\n", i, entity_vector[i].IsSpatial, entity_vector[i].location.x, entity_vector[i].location.y, entity_vector[i].type);
	}
	fclose(stdout);
}

void ReadEntityFromDisk(int &node_count, Entity Entity_Matrix[], int &range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	scanf("%d %d", &node_count, &range);
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d %lf %lf %d", &(Entity_Matrix[i].id), &(Entity_Matrix[i].IsSpatial), &(Entity_Matrix[i].location.x), &(Entity_Matrix[i].location.y), &(Entity_Matrix[i].type));
	}
	fclose(stdin);
}

void ReadEntityFromDisk(int &node_count, vector<Entity> &entity_vector, int &range, string filename)
{
	char *ch = (char *)filename.data();
	freopen(ch, "r", stdin);

	scanf("%d %d", &node_count, &range);
	entity_vector.resize(node_count);
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d %lf %lf %d", &(entity_vector[i].id), &(entity_vector[i].IsSpatial), &(entity_vector[i].location.x), &(entity_vector[i].location.y), &(entity_vector[i].type));
	}
	fclose(stdin);
}

void ReadEntityInSCCFromDisk(int &node_count, vector<Entity> &entity_vector, int &range, string filename)
{
	char *ch = (char *)filename.data();
	freopen(ch, "r", stdin);

	scanf("%d %d", &node_count, &range);
	entity_vector.resize(node_count);
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d %lf %lf %d %d %lf %lf %lf %lf", &(entity_vector[i].id), &(entity_vector[i].IsSpatial), &(entity_vector[i].location.x), &(entity_vector[i].location.y), &(entity_vector[i].type), &(entity_vector[i].scc_id), &(entity_vector[i].RMBR.left_bottom.x), &(entity_vector[i].RMBR.left_bottom.y), &(entity_vector[i].RMBR.right_top.x), &(entity_vector[i].RMBR.right_top.y));
	}
	fclose(stdin);
}

string getstring(const int i)
{
	stringstream newstr;
	newstr << i;
	return newstr.str();
}

int StringtoInt(string str)
{
	return atoi(str.c_str());
}

vector<string> split(string str, string pattern)
{
	vector<string> ret;
	if (pattern.empty()) return ret;
	size_t start = 0, index = str.find_first_of(pattern, 0);
	while (index != str.npos)
	{
		if (start != index)
			ret.push_back(str.substr(start, index - start));
		start = index + 1;
		index = str.find_first_of(pattern, start);
	}
	if (!str.substr(start).empty())
		ret.push_back(str.substr(start));
	return ret;
}

// A recursive function used by topologicalSort
void TopologicalSortUtil(int v, vector<bool> &visited, queue<int> &queue, vector<vector<int>> &graph)
{
	// Mark the current node as visited.
	visited[v] = true;

	// Recur for all the vertices adjacent to this vertex
	for (int i = 0; i < graph[v].size(); i++)
		if (!visited[graph[v][i]])
			TopologicalSortUtil(graph[v][i], visited, queue, graph);

	// Push current vertex to stack which stores result
	queue.push(v);
}

// The function to do Topological Sort. It uses recursive topologicalSortUtil()
void TopologicalSort(vector<vector<int>> &graph, queue<int> &queue)
{
	// Mark all the vertices as not visited
	vector<bool> visited = vector<bool>(graph.size());

	for (int i = 0; i < graph.size(); i++)
		visited[i] = false;

	// Call the recursive helper function to store Topological Sort
	// starting from all vertices one by one
	for (int i = 0; i < graph.size(); i++)
		if (visited[i] == false)
			TopologicalSortUtil(i, visited, queue, graph);
}

void ReadGraph(vector<vector<int>> &graph, int &node_count, string graph_filepath)
{
	char *ch = (char *)graph_filepath.data();
	freopen(ch, "r", stdin);

	scanf("%d\n", &node_count);
	graph.resize(node_count);

	for (int i = 0; i < node_count; i++)
	{
		int id, count, outid;
		scanf("%d\t%d\t", &id, &count);
		graph[i].resize(count);
		for (int j = 0; j < count; j++)
		{
			scanf("%d\t", &outid);
			graph[i][j] = outid;
		}
	}
	fclose(stdin);
}

void GenerateInedgeGraph(vector<vector<int>> &graph, vector<vector<int>> &in_edge_graph)
{
	int node_count = graph.size();
	in_edge_graph.resize(node_count);
	for (int i = 0; i < node_count; i++)
	{
		for (int j = 0; j < graph[i].size(); j++)
		{
			int neighbor = graph[i][j];
			in_edge_graph[neighbor].push_back(i);
		}
	}
}

