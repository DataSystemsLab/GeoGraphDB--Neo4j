#ifndef _GRAPHVECTOR_H_
#define _GRAPHVECTOR_H_
#include "Universe.h"

int GetPathsCount();
int GetEdgeCount();

//ArrayVector functions
void ArrayVectorToDisk(vector<edge> graph[], int node_count, string filename);
void ReadArrayVectorFromDisk(vector<edge> graph[], string filename);
void ReadArrayVectorFromDisk(vector<int> graph[], string filename);
void addvector(vector<edge> graph[], int start, int dest, int edge_type);
void Generate_ArrayVector(vector<edge> graph[], int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio);
void FindQualifiedPaths(vector<vector<int>> &Paths, vector<int> graph[], int vertex_num, int step_num, vector<int> vector_edge_type, vector<bool> spatialconstraint_step, vector<MyRect> constraint_rect, vector<Entity> entity_vector);

void VectorToDisk(vector<vector<int>> graph, string filename);
vector<vector<int>> ReadVectorFromDisk(string filename);
void addvector(vector<vector<int>> &graph, int start, int dest, int edge_type);
void Generate_Vector(vector<vector<int>> &graph, int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio);
void OutFile(vector<vector<int>> graph, string filename);


//Vector Vector funtions
//Without spatial constraints
vector<vector<int>> FindQualifiedPaths(vector<vector<int>> &graph, int vertex_num, int step_num, vector<int> vector_edge_type);

//With spatial constraints
vector<vector<int>> FindQualifiedPathsvector(vector<vector<int>> &graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<bool> spatialconstraint_step, vector<MyRect> constraint_rect, Entity entity_matrix[]);
void FindQualifiedPaths(vector<int> &Paths, vector<vector<int>> &graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<bool> spatialconstraint_step, vector<MyRect> constraint_rect, vector<Entity> &entity_vector);
void FindQualifiedPaths_testtime(vector<int> &Paths, vector<vector<int>> &graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<bool> spatialconstraint_step, vector<MyRect> constraint_rect, vector<Entity> &entity_vector);


//From start point to a specific end type through qualified edge type and steps
vector<vector<int>> FindQualifiedPaths(vector<vector<int>> &graph, int start_vertex_id, int end_type, int step_num, vector<int> vector_edge_type, Entity entity_matrix[]);
#endif