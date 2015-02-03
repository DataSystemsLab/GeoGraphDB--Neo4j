#include "stdafx.h"
#include "Graph.h"
int count = 0;
int m_node_count = 0;
/* Function to create an adjacency list node*/
adjlist_node_p createNode(int v, int edge_type)
{
	adjlist_node_p newNode = (adjlist_node_p)malloc(sizeof(adjlist_node_t));
	if (!newNode)
		err_exit("Unable to allocate memory for new node");

	newNode->vertex = v;
	newNode->next = NULL;

	newNode->edge_type = edge_type;//0:Friend         1:Family              2:Like              3:Visit               4:Liked                 5:Visted

	return newNode;
}

/* Function to create a graph with n vertices; Creates both directed and undirected graphs*/
graph_p createGraph(int n, graph_type_e type)
{
	int i;
	graph_p graph = (graph_p)malloc(sizeof(graph_t));
	if (!graph)
		err_exit("Unable to allocate memory for graph");
	graph->num_vertices = n;
	graph->type = type;

	/* Create an array of adjacency lists*/
	graph->adjListArr = (adjlist_p)malloc(n * sizeof(adjlist_t));
	if (!graph->adjListArr)
		err_exit("Unable to allocate memory for adjacency list array");

	for (i = 0; i < n; i++)
	{
		graph->adjListArr[i].head = NULL;
		graph->adjListArr[i].num_members = 0;
	}

	return graph;
}

/*Destroys the graph*/
void destroyGraph(graph_p graph)
{
	if (graph)
	{
		if (graph->adjListArr)
		{
			int v;
			/*Free up the nodes*/
			for (v = 0; v < graph->num_vertices; v++)
			{
				adjlist_node_p adjListPtr = graph->adjListArr[v].head;
				while (adjListPtr)
				{
					adjlist_node_p tmp = adjListPtr;
					adjListPtr = adjListPtr->next;
					free(tmp);
				}
			}
			/*Free the adjacency list array*/
			free(graph->adjListArr);
		}
		/*Free the graph*/
		free(graph);
	}
}

/* Adds an edge to a graph*/
void addEdge(graph_t *graph, int src, int dest, int edge_type)
{
	/* Add an edge from src to dst in the adjacency list*/
	adjlist_node_p newNode = createNode(dest, edge_type);
	newNode->next = graph->adjListArr[src].head;
	graph->adjListArr[src].head = newNode;
	graph->adjListArr[src].num_members++;

	if (graph->type == UNDIRECTED)
	{
		/* Add an edge from dest to src also*/
		newNode = createNode(src, edge_type);
		newNode->next = graph->adjListArr[dest].head;
		graph->adjListArr[dest].head = newNode;
		graph->adjListArr[dest].num_members++;
	}
}

/* Function to print the adjacency list of graph*/
void displayGraph(graph_p graph)
{
	int i;
	for (i = 0; i < graph->num_vertices; i++)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[i].head;
		printf("\n%d: ", i);
		while (adjListPtr)
		{
			if (adjListPtr->edge_type == 0)
				printf("Friend ");
			if (adjListPtr->edge_type == 1)
				printf("Family ");
			if (adjListPtr->edge_type == 2)
				printf("Like ");
			if (adjListPtr->edge_type == 3)
				printf("Visit ");
			if (adjListPtr->edge_type == 4)
				printf("Liked ");
			if (adjListPtr->edge_type == 5)
				printf("Visited ");
			if (adjListPtr->edge_type == 6)
				printf("Similar ");

			printf("%d->", adjListPtr->vertex);

			adjListPtr = adjListPtr->next;
		}
		printf("NULL\n");
	}
}

//Without spatial constraints
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type)
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				vector<adjlist_node_p> vector_Path;
				vector_Path.push_back(adjListPtr);
				vector_Paths.push_back(vector_Path);
			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(vertex_num);
		vector.push_back(vertex_num);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < vector.size(); i++)
						{
							int element = vector.at(i);
							if (adjListPtr->vertex == element)
							{
								adjListPtr = adjListPtr->next;
								break;
							}

							if (i == vector.size() - 1)
							{
								if (stack.size() == step_num)
								{
									stack.push(adjListPtr->vertex);
									vector.push_back(adjListPtr->vertex);
									vector_Path.push_back(adjListPtr);

									vector_Paths.push_back(vector_Path);

									adjListPtr = adjListPtr->next;
									stack.pop();
									vector.pop_back();
									vector_Path.pop_back();
								}
								else
								{
									vector.push_back(adjListPtr->vertex);
									stack.push(adjListPtr->vertex);
									vector_Path.push_back(adjListPtr);
									adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
									break;
								}
							}
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						vector.push_back(adjListPtr->vertex);
						stack.push(adjListPtr->vertex);
						vector_Path.push_back(adjListPtr);
						adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}

//With spatial constraints
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[])
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (constraint_rect.size() == 0)
	{
		vector_Paths = FindQualifiedPaths(graph, vertex_num, step_num, vector_edge_type);
		return vector_Paths;
	}

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				if (entity_matrix[adjListPtr->vertex].IsSpatial)
				{
 					if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[0]))
					{
						vector<adjlist_node_p> vector_Path;
						vector_Path.push_back(adjListPtr);
						vector_Paths.push_back(vector_Path);
					}
				}
				else
				{
					vector<adjlist_node_p> vector_Path;
					vector_Path.push_back(adjListPtr);
					vector_Paths.push_back(vector_Path);

				}

			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(vertex_num);
		vector.push_back(vertex_num);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < vector.size(); i++)
						{
							int element = vector.at(i);
							if (adjListPtr->vertex == element)
							{
								adjListPtr = adjListPtr->next;
								break;
							}

							if (i == vector.size() - 1)
							{
								//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
								bool flag_spatial = true;
								for (int j = 0; j < spatialconstraint_step_num.size(); j++)
								{
									//which step has the spatial constraint
									int constraint_step_num = spatialconstraint_step_num[j];
									//number of current step
									int current_num = vector_Path.size() + 1;
									if (constraint_step_num == current_num)
									{
										if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[j]))
											flag_spatial = true;
										else
											flag_spatial = false;
										break;
									}
								}

								if (flag_spatial)
								{
									if (stack.size() == step_num)
									{
										stack.push(adjListPtr->vertex);
										vector.push_back(adjListPtr->vertex);
										vector_Path.push_back(adjListPtr);

										vector_Paths.push_back(vector_Path);


										adjListPtr = adjListPtr->next;
										stack.pop();
										vector.pop_back();
										vector_Path.pop_back();
									}
									else
									{
										vector.push_back(adjListPtr->vertex);
										stack.push(adjListPtr->vertex);
										vector_Path.push_back(adjListPtr);
										adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
										break;
									}
								}
								else
								{
									adjListPtr = adjListPtr->next;
								}
							}
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					//out of the whole while rotation
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
						bool flag_spatial = true;
						for (int i = 0; i < spatialconstraint_step_num.size(); i++)
						{
							if (spatialconstraint_step_num[i] == 1)
							{
								if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[i]))
									flag_spatial = true;
								else
									flag_spatial = false;
								break;
							}
						}

						if (flag_spatial)
						{
							vector.push_back(adjListPtr->vertex);
							stack.push(adjListPtr->vertex);
							vector_Path.push_back(adjListPtr);
							adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
						}
						else
						{
							adjListPtr = adjListPtr->next;
						}

					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}

//From start point to a specific end type through qualified edge type and steps
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int start_vertex_id, int end_type, int step_num, vector<int> vector_edge_type, Entity entity_matrix[])
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[start_vertex_id].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				vector<adjlist_node_p> vector_Path;
				vector_Path.push_back(adjListPtr);
				vector_Paths.push_back(vector_Path);
			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[start_vertex_id].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(start_vertex_id);
		vector.push_back(start_vertex_id);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						//check whether this node exists in current path
						for (int i = 0; i < vector.size(); i++)
						{
							int element = vector.at(i);
							//exists so we go to check next node
							if (adjListPtr->vertex == element)
							{
								adjListPtr = adjListPtr->next;
								break;
							}

							//not exists
							if (i == vector.size() - 1)
							{
								//to the last step
								if (stack.size() == step_num)
								{
									//same with the end_vertex_id and this is the path what we want
									if (entity_matrix[adjListPtr->vertex].type == end_type)
									{
										stack.push(adjListPtr->vertex);
										vector.push_back(adjListPtr->vertex);
										vector_Path.push_back(adjListPtr);
										vector_Paths.push_back(vector_Path);
										stack.pop();
										vector.pop_back();
										vector_Path.pop_back();
										adjListPtr = adjListPtr->next;
									}
									//not same with the end vertex id
									else
									{
										adjListPtr = adjListPtr->next;
									}
								}

								//not the last step
								else
								{
									vector.push_back(adjListPtr->vertex);
									stack.push(adjListPtr->vertex);
									vector_Path.push_back(adjListPtr);
									adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
									break;
								}
							}
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						vector.push_back(adjListPtr->vertex);
						stack.push(adjListPtr->vertex);
						vector_Path.push_back(adjListPtr);
						adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}


//With spatial constraints,start from one type of nodes
vector<vector<adjlist_node_p>> FindQualifiedPaths_from_specific_entity_type(graph_p graph, int start_entity_type, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[])
{
	vector<vector<adjlist_node_p>> vector_Paths;
	::count = 0;

	for (int entity_id = 0; entity_id < m_node_count; entity_id++)
	{
		if (entity_matrix[entity_id].type == start_entity_type)
		{
			::count++;
			int vertex_num = entity_id;
			if (constraint_rect.size() == 0)
			{
				vector_Paths = FindQualifiedPaths(graph, vertex_num, step_num, vector_edge_type);
				return vector_Paths;
			}

			if (step_num == 1)
			{
				adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;

				while (adjListPtr)
				{
					if (adjListPtr->edge_type == vector_edge_type[0])
					{
						if (entity_matrix[adjListPtr->vertex].IsSpatial)
						{
							if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[0]))
							{
								vector<adjlist_node_p> vector_Path;
								vector_Path.push_back(adjListPtr);
								vector_Paths.push_back(vector_Path);
							}
						}
						else
						{
							vector<adjlist_node_p> vector_Path;
							vector_Path.push_back(adjListPtr);
							vector_Paths.push_back(vector_Path);

						}

					}
					adjListPtr = adjListPtr->next;
				}
				return vector_Paths;
			}

			if (step_num >= 2)
			{
				adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;
				stack<int> stack;
				vector<int> vector;

				::vector<adjlist_node_p> vector_Path;

				stack.push(vertex_num);
				vector.push_back(vertex_num);

				while (!stack.empty())
				{
					if (stack.size() <= step_num && stack.size() > 1)
					{
						if (!adjListPtr)
						{
							stack.pop();
							vector.pop_back();
							adjListPtr = vector_Path[vector_Path.size() - 1]->next;
							vector_Path.pop_back();
						}
						else
						{
							bool flag = false;
							//check whether this edge qualifies with edge type users request
							if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
							{
								flag = true;
							}

							if (flag)
							{
								//check whether this node exists in current path
								for (int i = 0; i < vector.size(); i++)
								{
									int element = vector.at(i);
									if (adjListPtr->vertex == element)
									{
										adjListPtr = adjListPtr->next;
										break;
									}

									if (i == vector.size() - 1)
									{
										//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
										bool flag_spatial = true;
										for (int j = 0; j < spatialconstraint_step_num.size(); j++)
										{
											//which step has the spatial constraint
											int constraint_step_num = spatialconstraint_step_num[j];
											//number of current step
											int current_num = vector_Path.size() + 1;
											if (constraint_step_num == current_num)
											{
												if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[j]))
													flag_spatial = true;
												else
													flag_spatial = false;
												break;
											}
										}

										if (flag_spatial)
										{
											if (stack.size() == step_num)
											{
												stack.push(adjListPtr->vertex);
												vector.push_back(adjListPtr->vertex);
												vector_Path.push_back(adjListPtr);

												vector_Paths.push_back(vector_Path);


												adjListPtr = adjListPtr->next;
												stack.pop();
												vector.pop_back();
												vector_Path.pop_back();
											}
											else
											{
												vector.push_back(adjListPtr->vertex);
												stack.push(adjListPtr->vertex);
												vector_Path.push_back(adjListPtr);
												adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
												break;
											}
										}
										else
										{
											adjListPtr = adjListPtr->next;
										}
									}
								}
							}
							else
								adjListPtr = adjListPtr->next;
						}
					}

					if (stack.size() == 1)
					{
						if (!adjListPtr)
						{
							//out of the whole while rotation
							break;
						}
						else
						{
							if (vector_edge_type[0] == adjListPtr->edge_type)
							{
								//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
								bool flag_spatial = true;
								for (int i = 0; i < spatialconstraint_step_num.size(); i++)
								{
									if (spatialconstraint_step_num[i] == 1)
									{
										if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[i]))
											flag_spatial = true;
										else
											flag_spatial = false;
										break;
									}
								}

								if (flag_spatial)
								{
									vector.push_back(adjListPtr->vertex);
									stack.push(adjListPtr->vertex);
									vector_Path.push_back(adjListPtr);
									adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
								}
								else
								{
									adjListPtr = adjListPtr->next;
								}

							}
							else
							{
								adjListPtr = adjListPtr->next;
							}
						}
					}
				}
			}
		}
	}
	return vector_Paths;
}


//With spatial constraints(allows closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[])
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (constraint_rect.size() == 0)
	{
		vector_Paths = FindQualifiedPaths(graph, vertex_num, step_num, vector_edge_type);
		return vector_Paths;
	}

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				if (entity_matrix[adjListPtr->vertex].IsSpatial)
				{
					if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[0]))
					{
						vector<adjlist_node_p> vector_Path;
						vector_Path.push_back(adjListPtr);
						vector_Paths.push_back(vector_Path);
					}
				}
				else
				{
					vector<adjlist_node_p> vector_Path;
					vector_Path.push_back(adjListPtr);
					vector_Paths.push_back(vector_Path);

				}

			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(vertex_num);
		vector.push_back(vertex_num);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
						bool flag_spatial = true;
						for (int j = 0; j < spatialconstraint_step_num.size(); j++)
						{
							//which step has the spatial constraint
							int constraint_step_num = spatialconstraint_step_num[j];
							//number of current step
							int current_num = vector_Path.size() + 1;
							if (constraint_step_num == current_num)
							{
								if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[j]))
									flag_spatial = true;
								else
									flag_spatial = false;
								break;
							}
						}

						if (flag_spatial)
						{
							if (stack.size() == step_num)
							{
								stack.push(adjListPtr->vertex);
								vector.push_back(adjListPtr->vertex);
								vector_Path.push_back(adjListPtr);

								vector_Paths.push_back(vector_Path);


								adjListPtr = adjListPtr->next;
								stack.pop();
								vector.pop_back();
								vector_Path.pop_back();
							}
							else
							{
								vector.push_back(adjListPtr->vertex);
								stack.push(adjListPtr->vertex);
								vector_Path.push_back(adjListPtr);
								adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
								//break;
							}
						}
						else
						{
							adjListPtr = adjListPtr->next;
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					//out of the whole while rotation
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						//check whether this step has spatial constraint and qualifies the spatial constraint(if there is no spatial constraint, the flag is true)
						bool flag_spatial = true;
						for (int i = 0; i < spatialconstraint_step_num.size(); i++)
						{
							if (spatialconstraint_step_num[i] == 1)
							{
								if (Location_In_Rect(entity_matrix[adjListPtr->vertex].location, constraint_rect[i]))
									flag_spatial = true;
								else
									flag_spatial = false;
								break;
							}
						}

						if (flag_spatial)
						{
							vector.push_back(adjListPtr->vertex);
							stack.push(adjListPtr->vertex);
							vector_Path.push_back(adjListPtr);
							adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
						}
						else
						{
							adjListPtr = adjListPtr->next;
						}

					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}

//From start point to a specific end through qualified edge type and steps(allow closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int start_vertex_id, int end_vertex_id, int step_num, vector<int> vector_edge_type)
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[start_vertex_id].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				vector<adjlist_node_p> vector_Path;
				vector_Path.push_back(adjListPtr);
				vector_Paths.push_back(vector_Path);
			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[start_vertex_id].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(start_vertex_id);
		vector.push_back(start_vertex_id);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						//to the last step
						if (stack.size() == step_num)
						{
							//same with the end_vertex_id and this is the path what we want
							if (adjListPtr->vertex == end_vertex_id)
							{
								stack.push(adjListPtr->vertex);
								vector.push_back(adjListPtr->vertex);
								vector_Path.push_back(adjListPtr);
								vector_Paths.push_back(vector_Path);
								stack.pop();
								vector.pop_back();
								vector_Path.pop_back();
								adjListPtr = adjListPtr->next;
							}
							//not same with the end vertex id
							else
							{
								adjListPtr = adjListPtr->next;
							}
						}

						//not the last step
						else
						{
							vector.push_back(adjListPtr->vertex);
							stack.push(adjListPtr->vertex);
							vector_Path.push_back(adjListPtr);
							adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						vector.push_back(adjListPtr->vertex);
						stack.push(adjListPtr->vertex);
						vector_Path.push_back(adjListPtr);
						adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}

//Without spatial constraints(allow closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type)
{
	vector<vector<adjlist_node_p>> vector_Paths;

	if (step_num == 1)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;

		while (adjListPtr)
		{
			if (adjListPtr->edge_type == vector_edge_type[0])
			{
				vector<adjlist_node_p> vector_Path;
				vector_Path.push_back(adjListPtr);
				vector_Paths.push_back(vector_Path);
			}
			adjListPtr = adjListPtr->next;
		}
		return vector_Paths;
	}

	if (step_num >= 2)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[vertex_num].head;
		stack<int> stack;
		vector<int> vector;

		::vector<adjlist_node_p> vector_Path;

		stack.push(vertex_num);
		vector.push_back(vertex_num);

		while (!stack.empty())
		{
			if (stack.size() <= step_num && stack.size() > 1)
			{
				if (!adjListPtr)
				{
					stack.pop();
					vector.pop_back();
					adjListPtr = vector_Path[vector_Path.size() - 1]->next;
					vector_Path.pop_back();
				}
				else
				{
					bool flag = false;
					//check whether this edge qualifies with edge type users request
					if (vector_edge_type[vector_Path.size()] == adjListPtr->edge_type)
					{
						flag = true;
					}

					if (flag)
					{
						if (stack.size() == step_num)
						{
							stack.push(adjListPtr->vertex);
							vector.push_back(adjListPtr->vertex);
							vector_Path.push_back(adjListPtr);

							vector_Paths.push_back(vector_Path);

							adjListPtr = adjListPtr->next;
							stack.pop();
							vector.pop_back();
							vector_Path.pop_back();
						}
						else
						{
							vector.push_back(adjListPtr->vertex);
							stack.push(adjListPtr->vertex);
							vector_Path.push_back(adjListPtr);
							adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
						}
					}
					else
						adjListPtr = adjListPtr->next;
				}
			}

			if (stack.size() == 1)
			{
				if (!adjListPtr)
				{
					break;
				}
				else
				{
					if (vector_edge_type[0] == adjListPtr->edge_type)
					{
						vector.push_back(adjListPtr->vertex);
						stack.push(adjListPtr->vertex);
						vector_Path.push_back(adjListPtr);
						adjListPtr = graph->adjListArr[adjListPtr->vertex].head;
					}
					else
					{
						adjListPtr = adjListPtr->next;
					}
				}
			}

		}
		return vector_Paths;
	}
}

//Display paths on the screen using printf
void DisplayPaths(int start_vertex, vector<vector<adjlist_node_p>> vector_Paths)
{
	if (vector_Paths.empty())
	{
		printf("No paths qualified!");
		return;
	}

	for (int i = 0; i < vector_Paths.size(); i++)
	{
		printf("%d", start_vertex);
		vector<adjlist_node_p> vector_path = vector_Paths.at(i);
		for (int j = 0; j < vector_path.size(); j++)
		{
			adjlist_node_p node = vector_path.at(j);
			printf(" -> %d", node->vertex);
		}
		printf("\n");
	}
}

//Display paths and edge type on the screen using printf
void DisplayPathsRelationship(int start_vertex, vector<vector<adjlist_node_p>> vector_Paths)
{
	if (vector_Paths.empty())
	{
		printf("No paths qualified!");
		return;
	}

	for (int i = 0; i < vector_Paths.size(); i++)
	{
		printf("%d", start_vertex);
		vector<adjlist_node_p> vector_path = vector_Paths.at(i);
		for (int j = 0; j < vector_path.size(); j++)
		{
			adjlist_node_p node = vector_path.at(j);
			printf(" -> ");
			if (node->edge_type == 0)
				printf("Friend ");
			if (node->edge_type == 1)
				printf("Family ");
			if (node->edge_type == 2)
				printf("Like ");
			if (node->edge_type == 3)
				printf("Visit ");
			if (node->edge_type == 4)
				printf("Liked ");
			if (node->edge_type == 5)
				printf("Visited ");
			if (node->edge_type == 6)
				printf("Similar ");
			printf("%d", node->vertex);
		}
		printf("\n");
	}
}

//Outfile graph to disk for storage with a -9999 in the end of every line
void GraphToDisk(graph_p graph, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", graph->num_vertices);
	for (int i = 0; i < graph->num_vertices; i++)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[i].head;
		printf("%d ", i);
		while (adjListPtr)
		{
			if (i == 4095)
			{
				int x = 0;
			}
			printf("%d ", adjListPtr->edge_type);
			printf("%d ", adjListPtr->vertex);
			adjListPtr = adjListPtr->next;
		}
		printf("-9999\n");
	}
	fclose(stdout);
}

//Read graph from disk to adacent list data structure
graph_p ReadGraphFromDisk(string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	int node_count = 0;

	int source_node, dest_node;
	int edge_type;

	scanf("%d", &node_count);
	graph_p	graph = createGraph(node_count, DIRECTED);

	while (true)
	{
		scanf("%d", &source_node);
		while (true)
		{
			scanf("%d", &edge_type);
			if (edge_type == -9999)
			{
				break;
			}
			scanf("%d", &dest_node);
			addEdge(graph, source_node, dest_node, edge_type);
		}

		if (feof(stdin))
			break;

	}
	return graph;
}

//Outfile graph to disk for displaying
void OutFile(graph_p graph, string filename)
{
	ofstream file;
	file.open("data/" + filename);
	for (int i = 0; i < graph->num_vertices; i++)
	{
		adjlist_node_p adjListPtr = graph->adjListArr[i].head;
		file << i << "  ";
		while (adjListPtr)
		{
			if (adjListPtr->edge_type == 0)
				file << "Friend ";
			if (adjListPtr->edge_type == 1)
				file << "Family ";
			if (adjListPtr->edge_type == 2)
				file << "Like ";
			if (adjListPtr->edge_type == 3)
				file << "Visit ";
			if (adjListPtr->edge_type == 4)
				file << "Liked ";
			if (adjListPtr->edge_type == 5)
				file << "Visited ";
			if (adjListPtr->edge_type == 6)
				file << "Similar ";
			file << adjListPtr->vertex << " ";
			adjListPtr = adjListPtr->next;
		}
		file << endl;
	}
	file.close();
}

//Generate graph with specific spatial entity ratio
graph_p Generate_Graph(int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio)
{
	int k = log2(node_count);
	node_count = pow(2, k);

	graph_p dir_graph = createGraph(node_count, DIRECTED);

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
		{
			continue;
		}
		if (i < node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addEdge(dir_graph, i, j, 0);
			addEdge(dir_graph, j, i, 0);
		}
		if (i < node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addEdge(dir_graph, i, j, 2);
			addEdge(dir_graph, j, i, 4);
		}
		if (i >= node_count*nonspatial_entity_ratio && j >= node_count*nonspatial_entity_ratio)
		{
			addEdge(dir_graph, i, j, 6);
			addEdge(dir_graph, j, i, 6);
		}
		if (i >= node_count*nonspatial_entity_ratio && j < node_count*nonspatial_entity_ratio)
		{
			addEdge(dir_graph, i, j, 4);
			addEdge(dir_graph, j, i, 2);
		}
	}
	return dir_graph;
}

