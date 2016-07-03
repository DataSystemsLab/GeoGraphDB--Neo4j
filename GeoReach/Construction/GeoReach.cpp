#include "stdafx.h"
#include "GeoReach.h"

int Return_resolution_offset(vector<int> &resolutions, vector<int> &offsets, int grid_id, int &offset)
{
	for (int i = 1; i < offsets.size(); i++)
		if (grid_id < offsets[i])
		{
		offset = offsets[i - 1];
		return resolutions[i - 1];
		}

	offset = offsets[offsets.size() - 1];
	return resolutions[resolutions.size() - 1];
}

void SetFalseRecursive(vector<vector<bool>> &index, vector<int> &resolutions, vector<int> &offsets, int id, int grid_id)
{
	int offset;
	int pieces = Return_resolution_offset(resolutions, offsets, grid_id, offset);
	if (pieces == resolutions[0])
	{
		index[id][grid_id] = false;
	}
	else
	{
		if (index[id][grid_id])
			index[id][grid_id] = false;
		else
		{
			int off_id = grid_id - offset;
			int m = off_id / pieces;
			int n = off_id - m*pieces;
			int mm = m * 2, nn = n * 2;
			int base = mm*pieces * 2 + nn + offset - pieces*pieces * 4;
			SetFalseRecursive(index, resolutions, offsets, id, base);
			SetFalseRecursive(index, resolutions, offsets, id, base + 1);
			SetFalseRecursive(index, resolutions, offsets, id, base + pieces * 2);
			SetFalseRecursive(index, resolutions, offsets, id, base + pieces * 2 + 1);
		}
	}
}

void SetFalseRecursive(vector<set<int>> &index, vector<int> &resolutions, vector<int> &offsets, int id, int grid_id)
{
	int offset;
	int pieces = Return_resolution_offset(resolutions, offsets, grid_id, offset);
	if (pieces == resolutions[0])
	{
		index[id].erase(grid_id);
	}
	else
	{
		if (index[id].find(grid_id) != index[id].end())
			index[id].erase(grid_id);
		else
		{
			int off_id = grid_id - offset;
			int m = off_id / pieces;
			int n = off_id - m*pieces;
			int mm = m * 2, nn = n * 2;
			int base = mm*pieces * 2 + nn + offset - pieces*pieces * 4;
			SetFalseRecursive(index, resolutions, offsets, id, base);
			SetFalseRecursive(index, resolutions, offsets, id, base + 1);
			SetFalseRecursive(index, resolutions, offsets, id, base + pieces * 2);
			SetFalseRecursive(index, resolutions, offsets, id, base + pieces * 2 + 1);
		}
	}
}

void Merge(vector<vector<bool>> &index, vector<int> &Types, int merge_count, int pieces_x, int pieces_y)
{
	int level_count = log2(pieces_x) + 1;
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int base = 0;
	int resolution = pieces_x;
	for (int i = 0; i < level_count; i++)
	{
		resolutions[i] = resolution;
		offsets[i] = base;

		base += resolution*resolution;
		resolution /= 2;
	}

	int offset = 0;
	for (int i = pieces_x; i >= 2; i /= 2)
	{
		offset += i*i;
		for (int id = 0; id < index.size(); id++)
		{
			if (Types[id] == 0)
			{
				for (int m = 0; m < i; m += 2)
				{
					for (int n = 0; n < i; n += 2)
					{
						int grid_id = m*i + n + offset - i*i;
						int true_count = 0;
						if (index[id][grid_id])
							true_count++;
						if (index[id][grid_id + 1])
							true_count++;
						if (index[id][grid_id + i])
							true_count++;
						if (index[id][grid_id + i + 1])
							true_count++;
						if (true_count >= merge_count)
						{
							int mm = m / 2, nn = n / 2;
							int high_level_grid_id = mm*i / 2 + nn;
							index[id][high_level_grid_id + offset] = true;
							SetFalseRecursive(index, resolutions, offsets, id, grid_id);
							SetFalseRecursive(index, resolutions, offsets, id, grid_id + 1);
							SetFalseRecursive(index, resolutions, offsets, id, grid_id + i);
							SetFalseRecursive(index, resolutions, offsets, id, grid_id + i + 1);
						}
					}
				}
			}
		}
	}
}

void Merge(vector<set<int>> &index, vector<int> &Types, int merge_count, int pieces_x, int pieces_y)
{
	int level_count = log2(pieces_x) + 1;
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int base = 0;
	int resolution = pieces_x;
	for (int i = 0; i < level_count; i++)
	{
		resolutions[i] = resolution;
		offsets[i] = base;

		base += resolution*resolution;
		resolution /= 2;
	}

	for (int j = 0; j < index.size(); j++)
	{
		if (Types[j] == 0)
		{
			for (set<int>::iterator iter = index[j].begin(); iter != index[j].end();)
			{
				int reach_grid_id = *iter;
				int offset;
				int pieces = Return_resolution_offset(resolutions, offsets, reach_grid_id, offset);
				int id = reach_grid_id - offset;
				int m = id / pieces, n = id - m*pieces;
				int mm = m / 2, nn = n / 2;
				m = mm * 2, n = nn * 2;
				int base = m*pieces + n + offset;

				int true_count = 0;
				set<int>::iterator end = index[j].end();
				if (index[j].find(base) != end)
					true_count++;
				if (index[j].find(base + 1) != end)
					true_count++;
				if (index[j].find(base + pieces) != end)
					true_count++;
				if (index[j].find(base + pieces + 1) != end)
					true_count++;
				if (true_count >= merge_count)
				{
					index[j].insert(offset + pieces*pieces + mm*pieces / 2 + nn);
					while (iter != end)
					{
						if (*iter == base || *iter == base + 1 || *iter == base + pieces + 1 || *iter == base + pieces)
							iter++;
						else
							break;
					}
					SetFalseRecursive(index, resolutions, offsets, j, base);
					SetFalseRecursive(index, resolutions, offsets, j, base + 1);
					SetFalseRecursive(index, resolutions, offsets, j, base + pieces);
					SetFalseRecursive(index, resolutions, offsets, j, base + pieces + 1);
				}
				else
					iter++;
			}
		}
	}
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
	//whether ReachGrid[start_id] has any reachable grid
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
					if (reach_count[start_id] > MG)
					{
						Types[start_id] = 1;
						return;
					}
				}
			}
			int start = clock();
			if (Types[end_id] == 0)
			{
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
								return;
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
								return;
							}
						}
					}
				}		
			}				
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
	ReadEntity(node_count, entity, entity_path);

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

	while (!Q.empty())
	{
		int id = Q.front();
		Q.pop();
		int start = clock();
		Types[id] = InitializeType(graph, Types, id, GeoB);
		if (Types[id] == 2)
			GeoB[id] = true;
		else
		{
			if (Types[id] == 0)
			{
				UpdateGVertex(graph, id, ReachGrid, reach_count, Types, entity, left_bottom, pieces_x, layer0_grid_count, resolution_x, resolution_y, MG);
			}

			UpdateRVertex(graph, id, RMBR, Types, entity);

			if (Types[id] == 1)
				if (RMBR[id].Area() >= total_area * MR)
				{
					Types[id] = 2;
					GeoB[id] = true;
				}
		}
	}
	
	if (MT != 0)
		Merge(ReachGrid, Types, MT, pieces_x, pieces_y);
	GeoReachToDisk(GeoReach_path, Types, ReachGrid, RMBR, GeoB);
}

void UpdateGVertex(vector<vector<int>> &graph, int start_id, vector<set<int>> &ReachGrid, vector<int> &Types, vector<Entity> &entity, Location &left_bottom, int pieces_x, double resolution_x, double resolution_y, int MG)
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
				if (ReachGrid[start_id].find(grid_id) == ReachGrid[start_id].end())
					ReachGrid[start_id].insert(grid_id);
				if (ReachGrid[start_id].size() > MG)
				{
					Types[start_id] = 1;
					return;
				}
				
			}
			int start = clock();
			if (Types[end_id] == 0)
			{
				set<int>::iterator end = ReachGrid[end_id].end();
				for (set<int>::iterator iter = ReachGrid[end_id].begin(); iter != end; iter++)
				{
					if (ReachGrid[start_id].find(*iter) == ReachGrid[start_id].end())
					{
						ReachGrid[start_id].insert(*iter);
						if (ReachGrid[start_id].size() > MG)
						{
							Types[start_id] = 1;
							return;
						}
					}
				}
			}
		}
	}
	if (!Flag)
		Types[start_id] = 2;
}

void GenerateGeoReachInSet(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y)
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
	ReadEntity(node_count, entity, entity_path);

	vector<int> Types = vector<int>(node_count);
	vector<set<int>> ReachGrid = vector<set<int>>(node_count);

	int grid_layer_count = log2(pieces_x);

	vector<MyRect> RMBR = vector<MyRect>(node_count);
	vector<bool> GeoB = vector<bool>(node_count);

	while (!Q.empty())
	{
		int id = Q.front();
		Q.pop();
		int start = clock();
		Types[id] = InitializeType(graph, Types, id, GeoB);
		if (Types[id] == 2)
			GeoB[id] = true;
		else
		{
			if (Types[id] == 0)
			{
				UpdateGVertex(graph, id, ReachGrid, Types, entity, left_bottom, pieces_x, resolution_x, resolution_y, MG);
			}

			UpdateRVertex(graph, id, RMBR, Types, entity);

			if (Types[id] == 1)
				if (RMBR[id].Area() >= total_area * MR)
				{
					Types[id] = 2;
					GeoB[id] = true;
				}
		}
	}

	if (MT != 0)
		Merge(ReachGrid, Types, MT, pieces_x, pieces_y);
	GeoReachToDisk(GeoReach_path, Types, ReachGrid, RMBR, GeoB);
}

void GeoReachToDisk(string GeoReach_path, vector<int> &Types, vector<vector<bool>> &ReachGrid, vector<MyRect> &RMBR, vector<bool> &GeoB)
{
	char* ch = (char*) GeoReach_path.data();
	freopen(ch, "w", stdout);
	for (int i = 0; i < Types.size(); i++)
	{
		printf("%d,%d", i, Types[i]);
		switch (Types[i])
		{
		case(0) :
		{
			for (int j = 0; j < ReachGrid[i].size(); j++)
				if (ReachGrid[i][j])
					printf(",%d", j);
			printf("\n");
			break;
		}
		case(1) :
		{
			printf(",%f,%f,%f,%f\n", RMBR[i].left_bottom.x, RMBR[i].left_bottom.y, RMBR[i].right_top.x, RMBR[i].right_top.y);
			break;
		}
		case(2) :
		{
			if (GeoB[i])
				printf(",1\n");
			else
				printf(",0\n");
		}
		default:
			break;
		}
	}
	fclose(stdout);
}

void GeoReachToDisk(string GeoReach_path, vector<int> &Types, vector<set<int>> &ReachGrid, vector<MyRect> &RMBR, vector<bool> &GeoB)
{
	char* ch = (char*)GeoReach_path.data();
	freopen(ch, "w", stdout);
	for (int i = 0; i < Types.size(); i++)
	{
		printf("%d,%d", i, Types[i]);
		switch (Types[i])
		{
		case(0) :
		{
			set<int>::iterator end = ReachGrid[i].end();
			for (set<int>::iterator iter = ReachGrid[i].begin(); iter != end; iter++)
				printf(",%d", *iter);
			printf("\n");
			break;
		}
		case(1) :
		{
			printf(",%f,%f,%f,%f\n", RMBR[i].left_bottom.x, RMBR[i].left_bottom.y, RMBR[i].right_top.x, RMBR[i].right_top.y);
			break;
		}
		case(2) :
		{
			if (GeoB[i])
				printf(",1\n");
			else
				printf(",0\n");
		}
		default:
			break;
		}
	}
	fclose(stdout);
}