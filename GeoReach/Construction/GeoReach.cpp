#include "stdafx.h"
#include "GeoReach.h"

int counter = 0;
int time_ReachGrid = 0;

void GenerateRMBR(string graph_path, string entity_path, string RMBR_path)
{

}

int InitializeType(vector<vector<int>> &graph, vector<int> &Types, int start_id, vector<bool> &GeoB)
{
	int type = 0;
	for (int i = 0; i < graph[start_id].size(); i++)
	{
		int end_id = graph[start_id][i];
		if (Types[end_id] == 2)
			if (GeoB[end_id])
				return 2;
			else
				continue;
		else
		{
			if (Types[end_id]>type)
				type = Types[end_id];
		}
	}
	return type;
}

void UpdateGVertex(vector<vector<int>> &graph, int start_id, vector<vector<bool>> &ReachGrid, vector<int> &reach_count, vector<int> &Types, vector<Entity> &entity, Location &left_bottom, int pieces_x, int layer0_grid_count, double resolution_x, double resolution_y, int MG)
{
	boolean Flag = false;

	for (int i = 0; i < graph[start_id].size(); i++)
	{
		int end_id = graph[start_id][i];
		//B-vertex with false value
		if (Types[end_id] == 2 && !entity[end_id].IsSpatial)
			continue;
		//G-vertex
		else
		{
			Flag = true;
			if (entity[end_id].IsSpatial)
			{
				int index_x = (entity[end_id].location.x - left_bottom.x) / resolution_x;
				int index_y = (entity[end_id].location.y - left_bottom.y) / resolution_y;
				int grid_id = index_x * pieces_x + index_y;
				if (!ReachGrid[start_id][grid_id])
				{
					reach_count[start_id]++;
					ReachGrid[start_id][grid_id] = true;
				}
			}
			int start = clock();
			if (Types[end_id] == 0)
			{
				counter++;
				if (layer0_grid_count - reach_count[start_id] < reach_count[end_id])
				{
					for (int j = 0; j < layer0_grid_count; j++)
					{
						if (!ReachGrid[start_id][j] && ReachGrid[end_id][j])
						{
							reach_count[start_id]++;
							ReachGrid[start_id][j] = true;
							if (reach_count[start_id] > MG)
							{
								Types[start_id] = 1;
								break;
							}
						}
					}
				}
				else
				{
					for (int j = 0; j < layer0_grid_count; j++)
					{
						if (ReachGrid[end_id][j] && !ReachGrid[start_id][j])
						{
							reach_count[start_id]++;
							ReachGrid[start_id][j] = true;
							if (reach_count[start_id] > MG)
							{
								Types[start_id] = 1;
								break;
							}
						}
					}
				}		
			}				
			time_ReachGrid += clock() - start;
		}
	}
	if (!Flag)
		Types[start_id] = 2;
}

void UpdateRVertex(vector<vector<int>> &graph, int start_id, vector<MyRect> &RMBR, vector<int> &Types, vector<Entity> &entity)
{
	for (int i = 0; i < graph[start_id].size(); i++)
	{
		int end_id = graph[start_id][i];
		if (entity[end_id].IsSpatial)
			RMBR[start_id].MBR(entity[end_id].location);
		if (Types[end_id] == 2)
			continue;
		else
			RMBR[start_id].MBR(RMBR[end_id]);
		
	}
}

void GenerateGeoReach(string graph_path, string entity_path, string GeoReach_path,  int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y)
{
	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;
	double total_area = (right_top.x - left_bottom.x) * (right_top.y - left_bottom.y);

	vector<vector<int>> graph;
	int node_count;
	ReadGraph(graph, node_count, graph_path);
	queue<int> Q;
	TopologicalSort(graph, Q);

	vector<Entity> entity;
	int range;
	ReadEntityInSCCFromDisk(node_count, entity, range, entity_path);

	vector<int> Types = vector<int>(node_count);
	vector<vector<bool>> ReachGrid = vector<vector<bool>>(node_count);

	int layer0_grid_count = pieces_x * pieces_x;
	int grid_layer_count = log2(pieces_x);
	int sum_grid_count = 0;
	for (int i = pieces_x; i >= 1; i /= 2)
		sum_grid_count += i*i;
	for (int i = 0; i < node_count; i++)
		ReachGrid[i].resize(sum_grid_count);
	vector<int> reach_count = vector<int>(node_count);
	vector<MyRect> RMBR = vector<MyRect>(node_count);
	vector<bool> GeoB = vector<bool>(node_count);

	ofstream ofile("time.txt", ios::app);
	int start = clock();
	int time_ini_type = 0, time_update_gvertex = 0, time_update_rvertex = 0;
	while (!Q.empty())
	{
		int id = Q.front();
		Q.pop();
		int start = clock();
		Types[id] = InitializeType(graph, Types, id, GeoB);
		time_ini_type += clock() - start;
		if (Types[id] == 2)
			GeoB[id] = true;
		else
		{
			if (Types[id] == 0)
			{
				int start = clock();
				UpdateGVertex(graph, id, ReachGrid, reach_count, Types, entity, left_bottom, pieces_x, layer0_grid_count, resolution_x, resolution_y, MG);
				time_update_gvertex += clock() - start;
			}

			int start = clock();
			UpdateRVertex(graph, id, RMBR, Types, entity);

			if (Types[id] == 1)
				if (RMBR[id].Area() >= total_area * MR)
				{
					Types[id] = 2;
					GeoB[id] = true;
				}
			time_update_rvertex += clock() - start;
		}
	}
	//ofile << "index time\t" << clock() - start << endl << "ini_type\t" << time_ini_type << endl << "update_G\t" << time_update_gvertex << endl << "update_R\t" << time_update_rvertex << endl;
	
	ofile << "index time\t" << clock() - start << endl << "ini_type\t" << time_ini_type << endl << "update_G\t" << time_update_gvertex << endl << "update_R\t" << time_update_rvertex << endl << "counter\t" << counter << endl << "ReachGrid\t" << time_ReachGrid << endl;
	start = clock();
	if (MT != 0)
		Merge(ReachGrid, Types, MT, pieces_x, pieces_y);
	ofile << "merge time\t" << clock() - start << endl << endl;
	ofile.close();
	//GeoReachToDisk(GeoReach_path, Types, ReachGrid, RMBR, GeoB);
}

void GenerateGeoReachFromInedgeGraph(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y)
{
	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;
	double total_area = (right_top.x - left_bottom.x) * (right_top.y - left_bottom.y);

	vector<vector<int>> graph, in_edge_graph;
	int node_count;
	ReadGraph(graph, node_count, graph_path);
	GenerateInedgeGraph(graph, in_edge_graph);
	queue<int> Q;
	TopologicalSort(graph, Q);

	vector<Entity> entity;
	int range;
	ReadEntityInSCCFromDisk(node_count, entity, range, entity_path);

	vector<int> Types = vector<int>(node_count);
	vector<vector<bool>> ReachGrid = vector<vector<bool>>(node_count);

	int layer0_grid_count = pieces_x * pieces_x;
	int grid_layer_count = log2(pieces_x);
	int sum_grid_count = 0;
	for (int i = pieces_x; i >= 1; i /= 2)
		sum_grid_count += i*i;
	for (int i = 0; i < node_count; i++)
		ReachGrid[i].resize(sum_grid_count);
	vector<int> reach_count = vector<int>(node_count);
	vector<MyRect> RMBR = vector<MyRect>(node_count);
	vector<bool> GeoB = vector<bool>(node_count);

	ofstream ofile("time.txt", ios::app);
	int start = clock();
	int time_ini_type = 0, time_update_gvertex = 0, time_update_rvertex = 0;

	for (int i = 0; i < entity.size(); i++)
	{
		if (entity[i].IsSpatial)
		{
			int m = (entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				int type = Types[in_neighbor];
				switch (type)
				{
				case(0) :
					if (!ReachGrid[in_neighbor][grid_id])
						ReachGrid[in_neighbor][grid_id] = true;
					reach_count[in_neighbor] += 1;
					if (reach_count[in_neighbor]>MG)
						Types[in_neighbor] = 1;
					type = 1;
				case(1) :
					RMBR[in_neighbor].MBR(entity[i].location);
					if (RMBR[in_neighbor].Area() > MR*total_area)
					{
						Types[in_neighbor] = 2;
						GeoB[in_neighbor] = true;
					}
					break;
				case(2) :
					break;
				}
			}
		}
	}
	int main_time = 0;
	int calculation_count = 0;
	while (!Q.empty())
	{
		int id = Q.front();
		Q.pop();

		if (reach_count[id] == 0)
		{
			Types[id] = 2;
			continue;
		}

		if (Types[id] == 2)
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int from_id = in_edge_graph[id][i];
				Types[from_id] = 2;
				GeoB[from_id] = true;
			}
		}
		else
		{ 
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int from_id = in_edge_graph[id][i];
				if (Types[from_id] == 2)
					continue;
				else
				{
					if (Types[from_id] == 1)
					{
						RMBR[from_id].MBR(RMBR[id]);
						if (RMBR[from_id].Area() >= MR*total_area)
						{
							RMBR[from_id].HasRec = false;
							Types[from_id] = 2;
							GeoB[from_id] = true;
						}
					}
					else
					{
						int start_main = clock();
						if (Types[id] == 0)
						{
							calculation_count++;
							for (int j = 0; j < layer0_grid_count; j++)
							{
								if (ReachGrid[id][j] && (!ReachGrid[from_id][j]))
								{
									ReachGrid[from_id][j] = true;
									reach_count[from_id]++;
									if (reach_count[from_id] >= MG)
									{
										Types[from_id] = 1;
										break;
									}
								}
							}
						}
						main_time += clock() - start_main;
						RMBR[from_id].MBR(RMBR[id]);
						if (Types[from_id] == 1)
						{
							if (RMBR[from_id].Area() >= MR*total_area)
							{
								RMBR[from_id].HasRec = false;
								Types[from_id] = 2;
								GeoB[from_id] = true;
							}
						}
					}
				}
			}
		}

		/*switch (Types[id])
		{
		case(0) :
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int from_id = in_edge_graph[id][i];
				int type = Types[from_id];
				switch (type)
				{
				case(0) :
				{
					for (int j = 0; j < layer0_grid_count; j++)
					{
						if (ReachGrid[id][j] && (!ReachGrid[from_id][j]))
						{
							ReachGrid[from_id][j] = true;
							reach_count[from_id]++;
							if (reach_count[from_id]>=MG)
							{
								Types[from_id] = 1;
								type = 1;
							}
						}
					}
				}
				case(1) :
				{

				}
				}
			}
		}
		}*/
	}
	//ofile << "index time\t" << clock() - start << endl << "ini_type\t" << time_ini_type << endl << "update_G\t" << time_update_gvertex << endl << "update_R\t" << time_update_rvertex << endl;
	
	ofile << "index time\t" << clock() - start << endl << "main time\t" << main_time << endl << "calculate_count\t" << calculation_count << endl;
	start = clock();
	if (MT != 0)
		Merge(ReachGrid, Types, MT, pieces_x, pieces_y);
	ofile << "merge time\t" << clock() - start << endl << endl;
	ofile.close();
	//GeoReachToDisk(GeoReach_path, Types, ReachGrid, RMBR, GeoB);
}

void GeoReachToDisk(string GeoReach_path, vector<int> &Types, vector<vector<bool>> &ReachGrid, vector<MyRect> &RMBR, vector<bool> &GeoB)
{
	char* ch = (char*) GeoReach_path.data();
	freopen(ch, "w", stdout);
	for (int i = 0; i < Types.size(); i++)
	{
		printf("%d %d ", i, Types[i]);
		switch (Types[i])
		{
		case(0) :
		{
			for (int j = 0; j < ReachGrid[i].size(); j++)
				if (ReachGrid[i][j])
					printf("%d ", j);
			printf("\n");
			break;
		}
		case(1) :
		{
			printf("%f %f %f %f\n", RMBR[i].left_bottom.x, RMBR[i].left_bottom.y, RMBR[i].right_top.x, RMBR[i].right_top.y);
			break;
		}
		case(2) :
		{
			if (GeoB[i])
				printf("1\n");
			else
				printf("0\n");
		}
		default:
			break;
		}
	}
	fclose(stdout);
}