#include "stdafx.h"
#include "GeoReach.h"

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

void UpdateGVertex(vector<vector<int>> &graph, int start_id, vector<vector<bool>> &ReachGrid, vector<int> &Types, vector<Entity> &entity, Location left_bottom, int pieces_x, double resolution_x, double resolution_y)
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
				ReachGrid[start_id][grid_id] = true;
			}
			if (Types[end_id] == 0)
				for (int j = 0; j < ReachGrid[start_id].size(); j++)
					ReachGrid[start_id][j] = ReachGrid[start_id][j] | ReachGrid[end_id][j];
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

	int grid_layer_count = log2(pieces_x);
	int sum_grid_count = 0;
	for (int i = pieces_x; i >= 1; i /= 2)
		sum_grid_count += i*i;
	for (int i = 0; i < node_count; i++)
		ReachGrid[i].resize(sum_grid_count);
	vector<MyRect> RMBR = vector<MyRect>(node_count);
	vector<bool> GeoB = vector<bool>(node_count);

	while (!Q.empty())
	{
		int id = Q.front();
		Q.pop();
		Types[id] = InitializeType(graph, Types, id, GeoB);
		if (Types[id] == 2)
			GeoB[id] = true;
		else
		{
			if (Types[id] == 0)
				UpdateGVertex(graph, id, ReachGrid, Types, entity, left_bottom, pieces_x, resolution_x, resolution_y);

			int reachgrid_count = 0;
			for (int i = 0; i < ReachGrid[id].size(); i++)
			{
				if (ReachGrid[id][i])
					reachgrid_count++;
			}
				
			if (reachgrid_count >= MG)
				Types[id] = 1;
			UpdateRVertex(graph, id, RMBR, Types, entity);
			if (RMBR[id].Area() >= total_area * MR)
			{
				Types[id] = 2;
				GeoB[id] = true;
			}
		}
	}
	Merge(ReachGrid, Types, MT, pieces_x, pieces_y);
	GeoReachToDisk(GeoReach_path, Types, ReachGrid, RMBR, GeoB);
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