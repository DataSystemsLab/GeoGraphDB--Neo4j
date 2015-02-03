#include "stdafx.h"
#include "GraphVector.h"

char* edge_index[7] = { "Friend", "Family", "Like", "Visit", "Liked", "Visited", "Similar" };

void ArrayVectorToDisk(vector<edge> graph[], int node_count, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", node_count);
	for (int i = 0; i < node_count; i++)
	{
		printf("%d %d ", i, graph[i].size());
		for (int j = 0; j < graph[i].size(); j++)
		{
			printf("%d ", graph[i][j].edge_type);
			printf("%d ", graph[i][j].vertex);
		}
		printf("\n");
	}
	fclose(stdout);
}

void ReadArrayVectorFromDisk(vector<edge> graph[], string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	int node_count = 0;

	int source_node, edge_type, dest_node, line_size;
	edge m_edge;

	scanf("%d", &node_count);

	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d", &source_node, &line_size);
		graph[i].reserve(line_size);
		for (int j = 0; j < line_size; j++)
		{
			scanf("%d %d", &edge_type, &dest_node);
			m_edge.edge_type = edge_type;
			m_edge.vertex = dest_node;
			graph[i].push_back(m_edge);
		}
	}
	fclose(stdin);
}

void addvector(vector<edge> graph[], int start, int dest, int edge_type)
{
	edge m_edge;
	m_edge.edge_type = edge_type;
	m_edge.vertex = dest;
	graph[start].push_back(m_edge);
}

void Generate_ArrayVector(vector<edge> graph[], int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio)
{
	int k = log2(node_count);
	node_count = pow(2, k);

	TRnd Rnd = time(0);

	for (INT64 m = 0; m < edge_count; m++)
	{

		int i = 0, j = 0;
		for (int t = 0; t < k; t++)
		{
			double prob = Rnd.GetUniDev();
			if (prob >= a&&prob < (a + b))
				j = j + pow(2, k - 1 - t);
			else
			{
				if (prob >= (a + b) && prob < (a + b + c))
				{
					i = i + pow(2, k - 1 - t);
				}
				else
				{
					if (prob >= (a + b + c))
					{
						i = i + pow(2, k - 1 - t);
						j = j + pow(2, k - 1 - t);
					}
				}
			}
		}
		if (i == j)
			continue;
		if (i < node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 0);
			addvector(graph, j, i, 0);
		}
		if (i < node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 2);
			addvector(graph, j, i, 4);
		}
		if (i >= node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 6);
			addvector(graph, j, i, 6);
		}
		if (i >= node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 4);
			addvector(graph, j, i, 2);
		}
	}
}

void OutFile(vector<vector<int>> graph, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", graph.size());
	for (int i = 0; i < graph.size(); i++)
	{
		printf("%d %d ", i, graph[i].size());
		for (int j = 0; j < graph[i].size(); j+=2)
		{
			printf(edge_index[graph[i][j]]);
			printf(" %d ", graph[i][j + 1]);
		}
		printf("\n");
	}
	fclose(stdout);
}

void VectorToDisk(vector<vector<int>> graph, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", graph.size());
	for (int i = 0; i < graph.size(); i++)
	{
		printf("%d %d ", i, graph[i].size());
		for (int j = 0; j < graph[i].size(); j++)
		{
			printf("%d ", graph[i][j]);
		}
		printf("\n");
	}
	fclose(stdout);
}

vector<vector<int>> ReadVectorFromDisk(string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	int node_count = 0;

	int source_node, edge_type, dest_node, line_size;
	edge m_edge;

	scanf("%d", &node_count);
	vector<vector<int>> graph;
	graph.resize(node_count);

	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d", &source_node, &line_size);
		graph[i].resize(line_size);

		for (int j = 0; j < line_size; j++)
		{
			scanf("%d", &graph[i][j]);
		}
	}
	fclose(stdin);
	return graph;
}

void addvector(vector<vector<int>> &graph, int start, int dest, int edge_type)
{
	graph[start].push_back(edge_type);
	graph[start].push_back(dest);
}

void Generate_Vector(vector<vector<int>> &graph, int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio)
{
	int k = log2(node_count);
	node_count = pow(2, k);

	graph.resize(node_count);
	int ratio = edge_count / node_count;
	for (int i = 0; i < node_count; i++)
	{
		graph[i].reserve(ratio*3);
	}

	TRnd Rnd = time(0);

	for (INT64 m = 0; m < edge_count; m++)
	{

		int i = 0, j = 0;
		for (INT64 t = 0; t < k; t++)
		{
			double prob = Rnd.GetUniDev();
			if (prob >= a&&prob < (a + b))
				j = j + pow(2, k - 1 - t);
			else
			{
				if (prob >= (a + b) && prob < (a + b + c))
				{
					i = i + pow(2, k - 1 - t);
				}
				else
				{
					if (prob >= (a + b + c))
					{
						i = i + pow(2, k - 1 - t);
						j = j + pow(2, k - 1 - t);
					}
				}
			}
		}
		if (i == j)
			continue;
		if (i < node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 0);
			addvector(graph, j, i, 0);
		}
		if (i < node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 2);
			addvector(graph, j, i, 4);
		}
		if (i >= node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 6);
			addvector(graph, j, i, 6);
		}
		if (i >= node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addvector(graph, i, j, 4);
			addvector(graph, j, i, 2);
		}
	}
}

vector<vector<int>> FindQualifiedPaths(vector<vector<int>> graph, int vertex_num, int step_num, vector<int> vector_edge_type)
{
	vector<vector<int>> Paths;

	if (step_num != vector_edge_type.size())
	{		
		MessageBox(NULL, _T("Input edge type vector is not consistant with step count! Please check it again."), _T("Error"), MB_OK);	
		return Paths;
	}
		
	if (graph[vertex_num].size() == 0)
	{
		return Paths;
	}

	if (step_num == 1)
	{
		for (int i = 1; i < graph[vertex_num].size(); i+=2)
		{
			if (graph[vertex_num][i - 1] == vector_edge_type[0])
			{
				vector<int> path;
				path.push_back(vertex_num);
				path.push_back(graph[vertex_num][i - 1]);
				path.push_back(graph[vertex_num][i]);
				Paths.push_back(path);
				continue;
			}
		}
		return Paths;
	}

	if (step_num >= 2)
	{
		vector<int> path_index;//index of nodes of current path we don't know whether it is qualified
		vector<int> path_node;//id of nodes of current path

		path_index.reserve(step_num + 1);
		path_node.reserve(step_num + 1);

		path_index.push_back(vertex_num);
		path_node.push_back(vertex_num);
		path_index.push_back(1);
		while (path_index.size() != 1)
		{
			if (path_index.size() <= step_num + 1 && path_index.size() > 1)
			{
				//current step has got to last node and we have to change former step node to another node to continue
				if (path_index[path_index.size() - 1] == graph[path_node[path_node.size() - 1]].size() + 1)
				{
					path_index.pop_back();
					path_node.pop_back();

					int position = path_index.size() - 1;
					path_index[position] += 2;
				}
				else
				{
					//check whether this edge qualifies with edge type users request
					bool flag = false;
					int position = path_index.size() - 1;
					int node = graph[path_node[position - 1]][path_index[position]];
					if (vector_edge_type[position - 1] == graph[path_node[position - 1]][path_index[position] - 1])
					{
						flag = true;
					}
					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < path_node.size(); i++)
						{
							//exsits, so we go to next node
							if (path_node[i] == node)
							{
								path_index[position] += 2;
								break;
							}

							//doesn't exist and we know this node is qualified
							if (i == path_node.size() - 1)
							{
								if (step_num == path_index.size() - 1)
								{
									path_node.push_back(node);
									vector<int> path;
									path.reserve(step_num * 2 + 1);
									path.push_back(vertex_num);
									for (int j = 1; j < path_index.size(); j++)
									{
										path.push_back(graph[path_node[j - 1]][path_index[j] - 1]);
										path.push_back(path_node[j]);
									}
									Paths.push_back(path);

									path_node.pop_back();

									path_index[position] += 2;
									break;
								}
								else
								{
									if (graph[node].size() == 0)
									{
										path_index[position] += 2;
										break;
									}
									else
									{
										path_node.push_back(node);
										path_index.push_back(1);
										break;
									}
								}
							}

						}
					}
					else
						path_index[path_index.size() - 1] += 2;
				}
			}
		}
	}
	return Paths;
}

vector<vector<int>> FindQualifiedPaths(vector<vector<int>> graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<bool> spatialconstraint_step, vector<MyRect> constraint_rect, Entity entity_matrix[])
{
	vector<vector<int>> Paths;

	if (step_num != vector_edge_type.size())
	{
		MessageBox(NULL, _T("Input edge type vector is not consistant with step count! Please check it again."), _T("Error"), MB_OK);
		return Paths;
	}

	//Without spatial constraint we use another function which implements the nonspatial constraint search
	if (constraint_rect.size() == 0)
	{
		Paths = FindQualifiedPaths(graph, vertex_num, step_num, vector_edge_type);
		return Paths;
	}

	//start node has no next nodes we directly return(although the possibility is very low)
	if (graph[vertex_num].size() == 0)
	{
		return Paths;
	}

	//one step condition
	if (step_num == 1)
	{
		for (int i = 1; i < graph[vertex_num].size(); i += 2)
		{
			if (graph[vertex_num][i - 1] == vector_edge_type[0])
			{
				if (Location_In_Rect(entity_matrix[graph[vertex_num][i]].location, constraint_rect[0]))
				{
					vector<int> path;
					path.push_back(vertex_num);
					path.push_back(graph[vertex_num][i - 1]);
					path.push_back(graph[vertex_num][i]);
					Paths.push_back(path);
					continue;
				}
			}
		}
		return Paths;
	}

	if (step_num >= 2)
	{
		vector<int> path_index;//index of nodes of current path we don't know whether it is qualified
		vector<int> path_node;//id of nodes of current path

		path_index.reserve(step_num + 1);
		path_node.reserve(step_num + 1);

		path_index.push_back(vertex_num);
		path_node.push_back(vertex_num);
		path_index.push_back(1);
		while (path_index.size() != 1)
		{
			if (path_index.size() <= step_num + 1 && path_index.size() > 1)
			{
				//current step has got to last node and we have to change former step node to another node to continue
				if (path_index[path_index.size() - 1] == graph[path_node[path_node.size() - 1]].size() + 1)
				{
					path_index.pop_back();
					path_node.pop_back();

					int position = path_index.size() - 1;
					path_index[position] += 2;
				}
				else
				{
					//check whether this edge qualifies with edge type users request
					bool flag = false;
					int position = path_index.size() - 1;
					int node = graph[path_node[position - 1]][path_index[position]];
					if (vector_edge_type[position - 1] == graph[path_node[position - 1]][path_index[position] - 1])
					{
						if (spatialconstraint_step[position])
						{
							if (Location_In_Rect(entity_matrix[graph[path_node[position - 1]][path_index[position]]].location, constraint_rect[position]))
							{
								flag = true;
							}
							else
								flag = false;
						}
						else
							flag = true;
					}
					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < path_node.size(); i++)
						{
							//exsits, so we go to next node
							if (path_node[i] == node)
							{
								path_index[position] += 2;
								break;
							}

							//doesn't exist and we know this node is qualified
							if (i == path_node.size() - 1)
							{
								if (step_num == path_index.size() - 1)
								{
									path_node.push_back(node);
									vector<int> path;
									path.reserve(step_num * 2 + 1);
									path.push_back(vertex_num);
									for (int j = 1; j < path_index.size(); j++)
									{
										path.push_back(graph[path_node[j - 1]][path_index[j] - 1]);
										path.push_back(path_node[j]);
									}
									Paths.push_back(path);

									path_node.pop_back();

									path_index[position] += 2;
									break;
								}
								else
								{
									if (graph[node].size() == 0)
									{
										path_index[position] += 2;
										break;
									}
									else
									{
										path_node.push_back(node);
										path_index.push_back(1);
										break;
									}
								}
							}

						}
					}
					else
						path_index[path_index.size() - 1] += 2;
				}
			}
		}
		return Paths;
	}

}

vector<vector<int>> FindQualifiedPaths(vector<vector<int>> graph, int start_vertex_id, int end_type, int step_num, vector<int> vector_edge_type, Entity entity_matrix[])
{
	vector<vector<int>> Paths;

	if (step_num != vector_edge_type.size())
	{
		MessageBox(NULL, _T("Input edge type vector is not consistant with step count! Please check it again."), _T("Error"), MB_OK);
		return Paths;
	}

	if (graph[start_vertex_id].size() == 0)
	{
		return Paths;
	}

	if (step_num == 1)
	{
		for (int i = 1; i < graph[start_vertex_id].size(); i += 2)
		{
			if ((graph[start_vertex_id][i - 1] == vector_edge_type[0]) && (entity_matrix[graph[start_vertex_id][i]].type == end_type))
			{
				vector<int> path;
				path.push_back(start_vertex_id);
				path.push_back(graph[start_vertex_id][i - 1]);
				path.push_back(graph[start_vertex_id][i]);
				Paths.push_back(path);
				continue;
			}
		}
		return Paths;
	}

	if (step_num >= 2)
	{
		vector<int> path_index;//index of nodes of current path we don't know whether it is qualified
		vector<int> path_node;//id of nodes of current path

		path_index.reserve(step_num + 1);
		path_node.reserve(step_num + 1);

		path_index.push_back(start_vertex_id);
		path_node.push_back(start_vertex_id);
		path_index.push_back(1);
		while (path_index.size() != 1)
		{
			if (path_index.size() <= step_num + 1 && path_index.size() > 1)
			{
				//current step has got to last node and we have to change former step node to another node to continue
				if (path_index[path_index.size() - 1] == graph[path_node[path_node.size() - 1]].size() + 1)
				{
					path_index.pop_back();
					path_node.pop_back();

					int position = path_index.size() - 1;
					path_index[position] += 2;
				}
				else
				{
					//check whether this edge qualifies with edge type and node type that users request
					bool flag = false;
					int position = path_index.size() - 1;
					int node = graph[path_node[position - 1]][path_index[position]];
					if (vector_edge_type[position - 1] == graph[path_node[position - 1]][path_index[position] - 1])
					{
						flag = true;
					}
					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < path_node.size(); i++)
						{
							//exsits, so we go to next node
							if (path_node[i] == node)
							{
								path_index[position] += 2;
								break;
							}

							//doesn't exist and we know this node is qualified
							if (i == path_node.size() - 1)
							{
								if (step_num == path_index.size() - 1)
								{
									if (entity_matrix[node].type == end_type)
									{
										path_node.push_back(node);
										vector<int> path;
										path.reserve(step_num * 2 + 1);
										path.push_back(start_vertex_id);
										for (int j = 1; j < path_index.size(); j++)
										{
											path.push_back(graph[path_node[j - 1]][path_index[j] - 1]);
											path.push_back(path_node[j]);
										}
										Paths.push_back(path);

										path_node.pop_back();
									}
									path_index[position] += 2;
									break;
								}
								else
								{
									if (graph[node].size() == 0)
									{
										path_index[position] += 2;
										break;
									}
									else
									{
										path_node.push_back(node);
										path_index.push_back(1);
										break;
									}
								}
							}

						}
					}
					else
						path_index[path_index.size() - 1] += 2;
				}
			}
		}
	}
	return Paths;
}