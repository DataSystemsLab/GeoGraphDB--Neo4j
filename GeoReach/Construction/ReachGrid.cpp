#include "stdafx.h"
#include "ReachGrid.h"

void GenerateReachGrid(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored)
{

}

void Merge(vector<vector<bool>> &index, vector<bool> &IsStored, int merge_count, int pieces_x, int pieces_y)
{
	int level_count = log2(pieces_x);
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int resolution = pieces_x;


	int offset = 0;
	for (int i = pieces_x; i >= 2; i /= 2)
	{
		offset += i*i;
		for (int id = 0; id < index.size(); id++)
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

void Merge(vector<set<int>> &index, vector<bool> &IsStored, int merge_count, int pieces_x, int pieces_y)
{
	int level_count = log2(pieces_x);
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int base = 0;
	for (int i = 0; i < level_count; i++)
	{
		int resolution = pieces_x;	

		resolutions[i] = resolution;
		offsets[i] = base;

		base += resolution*resolution;
		resolution /= 2;
	}

	for (int j = 0; j < index.size(); j++)
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
				while ((*iter == base || *iter == base + 1 || *iter == base + pieces + 1 || *iter == base + pieces) && iter != end)
				{
					iter++;
				}
				SetFalseRecursive(index, resolutions, offsets, j, base);
				SetFalseRecursive(index, resolutions, offsets, j, base + 1);
				SetFalseRecursive(index, resolutions, offsets, j, base + pieces);
				SetFalseRecursive(index, resolutions, offsets, j, base + pieces + 1);

				index[j].insert(offset + pieces*pieces + mm*pieces / 2 + nn);
			}
			else
				iter++;
		}
	}
}

void Merge(vector<vector<bool>> &index, vector<int> &Types, int merge_count, int pieces_x, int pieces_y)
{
	int level_count = log2(pieces_x);
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
	int level_count = log2(pieces_x);
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int base = 0;
	for (int i = 0; i < level_count; i++)
	{
		int resolution = pieces_x;

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
					while ((*iter == base || *iter == base + 1 || *iter == base + pieces + 1 || *iter == base + pieces) && iter != end)
					{
						iter++;
					}
					SetFalseRecursive(index, resolutions, offsets, j, base);
					SetFalseRecursive(index, resolutions, offsets, j, base + 1);
					SetFalseRecursive(index, resolutions, offsets, j, base + pieces);
					SetFalseRecursive(index, resolutions, offsets, j, base + pieces + 1);

					index[j].insert(offset + pieces*pieces + mm*pieces / 2 + nn);
				}
				else
					iter++;
			}
		}
	}
}

void GenerateGridPointIndexPartialSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<int> ReachCount = vector<int>(in_edge_graph.size());
	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				if (index[in_neighbor][grid_id] == false)
				{
					index[in_neighbor][grid_id] = true;
					ReachCount[in_neighbor] += 1;
					if (ReachCount[in_neighbor]>threshold)
						IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}		
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			if (IsStored[id])
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];

					//grid union
					for (int j = 0; j < grid_count; j++)
					{
						if (index[id][j] && (!index[in_neighbor][j]))
						{
							IsUpdate[in_neighbor] = true;
							index[in_neighbor][j] = true;
							ReachCount[in_neighbor] += 1;
							if (ReachCount[in_neighbor] > threshold)
								IsStored[in_neighbor] = false;
						}
					}
				}
			}
			else
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
		else
			IsStored[id] = false;
	}
}

void GenerateGridPointIndexPartialSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				set<int>::iterator end = index[in_neighbor].end();
				if (index[in_neighbor].find(grid_id) == end)
				{
					index[in_neighbor].insert(grid_id);
					if (index[in_neighbor].size()>threshold)
						IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			if (IsStored[id])
			{
				set<int>::iterator id_end = index[id].end();
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					for (set<int>::iterator iter = index[id].begin(); iter != id_end; iter++)
					{
						//grid union
						if (index[in_neighbor].find(*iter) == index[in_neighbor].end())
						{
							IsUpdate[in_neighbor] = true;
							index[in_neighbor].insert(*iter);
							if (index[in_neighbor].size() > threshold)
								IsStored[in_neighbor] = false;
						}
					}
				}
			}
			else
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
		else
			IsStored[id] = false;
	}
}

void GenerateMultilevelGridPointIndex(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored, int merge_count)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<int> ReachCount = vector<int>(in_edge_graph.size());
	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				if (index[in_neighbor][grid_id] == false)
				{
					index[in_neighbor][grid_id] = true;
					ReachCount[in_neighbor] += 1;
					if (ReachCount[in_neighbor]>threshold)
						IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			if (IsStored[id])
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];

					//grid union
					for (int j = 0; j < grid_count; j++)
					{
						if (index[id][j] && (!index[in_neighbor][j]))
						{
							IsUpdate[in_neighbor] = true;
							index[in_neighbor][j] = true;
							ReachCount[in_neighbor] += 1;
							if (ReachCount[in_neighbor] > threshold)
								IsStored[in_neighbor] = false;
						}
					}
				}
			}
			else
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
		else
			IsStored[id] = false;
	}

	int offset = 0;
	for (int i = pieces_x; i >=2 ; i/=2)
	{
		offset += i*i;
		for (int id = 0; id < in_edge_graph.size(); id++)
		{
			if (IsStored[id])
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
							index[id][grid_id] = false;
							index[id][grid_id + 1] = false;
							index[id][grid_id + i] = false;
							index[id][grid_id + i + 1] = false;
						}
					}
				}
			}			
		}
	}
}

void GenerateMultilevelGridPointIndex(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored, int merge_count)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				set<int>::iterator end = index[in_neighbor].end();
				if (index[in_neighbor].find(grid_id) == end)
				{
					index[in_neighbor].insert(grid_id);
					if (index[in_neighbor].size()>threshold)
						IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			if (IsStored[id])
			{
				set<int>::iterator id_end = index[id].end();
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					for (set<int>::iterator iter = index[id].begin(); iter != id_end; iter++)
					{
						//grid union
						if (index[in_neighbor].find(*iter) == index[in_neighbor].end())
						{
							IsUpdate[in_neighbor] = true;
							index[in_neighbor].insert(*iter);
							if (index[in_neighbor].size() > threshold)
								IsStored[in_neighbor] = false;
						}
					}
				}
			}
			else
			{
				for (int i = 0; i < in_edge_graph[id].size(); i++)
				{
					int in_neighbor = in_edge_graph[id][i];
					IsStored[in_neighbor] = false;
					IsUpdate[in_neighbor] = true;
				}
			}
		}
		else
			IsStored[id] = false;
	}
	Merge(index, IsStored, merge_count, pieces_x, pieces_y);
}

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
	if (pieces == 128)
	{
		index[id].erase(grid_id);
	}
	else
	{
		if (index[id].find(grid_id)!=index[id].end())
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

void GenerateMultilevelGridPointIndexFull(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int merge_count)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				index[in_neighbor][grid_id] = true;
				IsUpdate[in_neighbor] = true;
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int in_neighbor = in_edge_graph[id][i];

				//grid union
				for (int j = 0; j < grid_count; j++)
				{
					if (index[id][j])
					{
						index[in_neighbor][j] = true;
						IsUpdate[in_neighbor] = true;
					}
				}
			}
		}
	}

	vector<bool> IsStored = vector<bool>(index.size());
	for (int i = 0; i < index.size(); i++)
		IsStored[i] = true;

	Merge(index, IsStored, merge_count, pieces_x, pieces_y);
	/*int offset = 0;
	for (int i = pieces_x; i > 2; i /= 2)
	{
		offset += i*i;
		for (int id = 0; id < in_edge_graph.size(); id++)
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
						SetFalseRecursive(index, id, grid_id);
						SetFalseRecursive(index, id, grid_id + 1);
						SetFalseRecursive(index, id, grid_id + i);
						SetFalseRecursive(index, id, grid_id + i + 1);
					}
				}
			}
		}
	}*/
}

void GenerateMultilevelGridPointIndexFull(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int merge_count)
{
	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				index[in_neighbor].insert(grid_id);
				IsUpdate[in_neighbor] = true;
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int in_neighbor = in_edge_graph[id][i];

				//grid union
				set<int>::iterator end = index[id].end();
				for (set<int>::iterator iter = index[id].begin(); iter != end; iter++)
				{
					if (index[in_neighbor].find(*iter) == index[in_neighbor].end())
					{
						index[in_neighbor].insert(*iter);
						IsUpdate[in_neighbor] = true;
					}
				}
			}
		}
	}

	int level_count = log2(pieces_x);
	vector<int> resolutions = vector<int>(level_count);
	vector<int> offsets = vector<int>(level_count);
	int base = 0;
	for (int i = 0; i < level_count; i++)
	{
		int resolution = pieces_x;

		resolutions[i] = resolution;
		offsets[i] = base;

		base += resolution*resolution;
		resolution /= 2;
	}

	vector<bool> IsStored = vector<bool>(index.size());
	for (int i = 0; i < index.size(); i++)
		IsStored[i] = true;

	Merge(index, IsStored, merge_count, pieces_x, pieces_y);
	/*for (int j = 0; j < in_edge_graph.size(); j++)
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
				while ((*iter == base || *iter == base + 1 || *iter == base + pieces + 1 || *iter == base + pieces) && iter != end)
				{
					iter++;
				}
				SetFalseRecursive(index, j, base);
				SetFalseRecursive(index, j, base + 1);
				SetFalseRecursive(index, j, base + pieces);
				SetFalseRecursive(index, j, base + pieces + 1);

				index[j].insert(offset + pieces*pieces + mm*pieces / 2 + nn);
			}
			else
				iter++;
		}
	}*/
}

void GridPointIndexToDiskSkip(vector<vector<bool>> &index, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		int count = 0;
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				count++;
		}

		if (count == 0)
			continue;

		printf("%d %d ", i, count);
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				printf("%d ", j);
		}
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDiskSkip(vector<set<int>> &index, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		int count = index[i].size();

		if (count == 0)
			continue;

		printf("%d %d ", i, count);
		set<int>::iterator end = index[i].end();
		for (set<int>::iterator iter = index[i].begin(); iter != end;iter++)
		{
			printf("%d ", *iter);
		}
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDisk(vector<vector<bool>> &index, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		int count = 0;
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				count++;
		}

		printf("%d %d ", i, count);
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				printf("%d ", j);
		}
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDisk(vector<set<int>> &index, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		int count = index[i].size();

		printf("%d %d ", i, count);
		set<int>::iterator end = index[i].end();
		for (set<int>::iterator iter = index[i].begin(); iter != end; iter++)
		{
			printf("%d ", *iter);
		}
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDisk(vector<vector<bool>> &index, string filename, vector<bool> &IsStored)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		if (IsStored[i])
		{
			int count = 0;
			for (int j = 0; j < index[i].size(); j++)
			{
				if (index[i][j])
					count++;
			}

			printf("%d %d ", i, count);
			for (int j = 0; j < index[i].size(); j++)
			{
				if (index[i][j])
					printf("%d ", j);
			}
		}
		else
			continue;
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDisk(vector<set<int>> &index, string filename, vector<bool> &IsStored)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", index.size());

	for (int i = 0; i < index.size(); i++)
	{
		if (IsStored[i])
		{
			printf("%d %d ", i, index[i].size());
			set<int>::iterator end = index[i].end();
			for (set<int>::iterator iter = index[i].begin(); iter != end; iter++)
			{
				printf("%d ", *iter);
			}
		}
		else
			continue;
		printf("\n");
	}
	fclose(stdout);
}

void GridPointIndexToDisk(vector<vector<bool>> &index, string filename,vector<int> stored, int total_count)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", total_count);

	for (int i = 0; i < stored.size(); i++)
	{
		int id = stored[i];
		int count = 0;
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				count++;
		}
		printf("%d %d ", id, count);
		for (int j = 0; j < index[i].size(); j++)
		{
			if (index[i][j])
				printf("%d ", j);
		}
		printf("\n");
	}

	fclose(stdout);
}

void GridPointIndexToDisk(vector<set<int>> &index, string filename, vector<int> stored, int total_count)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d\n", total_count);

	for (int i = 0; i < stored.size(); i++)
	{
		int id = stored[i];
		int count = index[i].size();

		printf("%d %d ", id, count);

		set<int>::iterator end = index[i].end();
		for (set<int>::iterator iter = index[i].begin(); iter != end; iter++)
		{
			printf("%d ", *iter);
		}
		printf("\n");
	}
	fclose(stdout);
}

void GenerateGridIndexSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				index[in_neighbor][grid_id] = true;
				IsUpdate[in_neighbor] = true;
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int in_neighbor = in_edge_graph[id][i];

				//grid union
				for (int j = 0; j < grid_count; j++)
				{
					if (index[id][j])
					{
						index[in_neighbor][j] = true;
						IsUpdate[in_neighbor] = true;
					}
				}
			}
		}
	}
}

void GenerateGridIndexSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue)
{
	int grid_count = pieces_x*pieces_y;

	double resolution_x = (right_top.x - left_bottom.x) / pieces_x;
	double resolution_y = (right_top.y - left_bottom.y) / pieces_y;

	vector<bool> IsUpdate = vector<bool>(in_edge_graph.size());

	for (int i = 0; i < p_entity.size(); i++)
	{
		if (p_entity[i].IsSpatial)
		{
			int m = (p_entity[i].location.x - left_bottom.x) / resolution_x;
			int n = (p_entity[i].location.y - left_bottom.y) / resolution_y;
			int grid_id = m*pieces_x + n;
			for (int j = 0; j < in_edge_graph[i].size(); j++)
			{
				int in_neighbor = in_edge_graph[i][j];
				index[in_neighbor].insert(grid_id);
				IsUpdate[in_neighbor] = true;
			}
		}
	}

	while (!queue.empty())
	{
		int id = queue.front();
		queue.pop();
		if (IsUpdate[id])
		{
			for (int i = 0; i < in_edge_graph[id].size(); i++)
			{
				int in_neighbor = in_edge_graph[id][i];

				//grid union
				set<int>::iterator end = index[id].end();
				for (set<int>::iterator iter = index[id].begin(); iter != end; iter++)
				{
					if (index[in_neighbor].find(*iter) == index[in_neighbor].end())
					{
						index[in_neighbor].insert(*iter);
						IsUpdate[in_neighbor] = true;
					}
				}
			}
		}
	}
}
