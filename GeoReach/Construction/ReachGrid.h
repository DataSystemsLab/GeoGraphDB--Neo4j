/*Grid.h*/
#ifndef _GRID.H_
#define _GRID_H_

#include "Universe.h"

void Merge(vector<vector<bool>> &index, vector<bool> &IsStored, int merge_count, int pieces_x, int pieces_y);
void Merge(vector<set<int>> &index, vector<bool> &IsStored, int merge_count, int pieces_x, int pieces_y);

void Merge(vector<vector<bool>> &index, vector<int> &Types, int merge_count, int pieces_x, int pieces_y);

/*GeoF*/
//using bool to store index 
void GenerateGridIndexSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue);
//using int list to store index
void GenerateGridIndexSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue);

/*GeoP*/
void GenerateGridPointIndexPartialSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored);
void GenerateGridPointIndexPartialSequence(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored);

int Return_resolution_offset(vector<int> &resolutions, vector<int> &offsets, int grid_id, int &offset);

/*GeoMT*/
void GenerateMultilevelGridPointIndex(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored, int merge_count);
void GenerateMultilevelGridPointIndex(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int threshold, vector<bool> &IsStored, int merge_count);

//no threshold
void GenerateMultilevelGridPointIndexFull(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<vector<bool>> &index, queue<int> &queue, int merge_count);
void GenerateMultilevelGridPointIndexFull(Location left_bottom, Location right_top, int pieces_x, int pieces_y, vector<vector<int>> &in_edge_graph, vector<Entity> &p_entity, vector<set<int>> &index, queue<int> &queue, int merge_count);

void SetFalseRecursive(vector<vector<bool>> &index, vector<int> &resolutions, vector<int> &offsets, int id, int grid_id);
void SetFalseRecursive(vector<set<int>> &index, vector<int> &resolutions, vector<int> &offsets, int id, int grid_id);

void GridPointIndexToDisk(vector<vector<bool>> &index, string filename);
void GridPointIndexToDisk(vector<set<int>> &index, string filename);
void GridPointIndexToDisk(vector<vector<bool>> &index, string filename, vector<bool> &IsStored);
void GridPointIndexToDisk(vector<set<int>> &index, string filename, vector<bool> &IsStored);

#endif