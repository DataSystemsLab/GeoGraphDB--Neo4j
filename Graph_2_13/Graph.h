/*graph.h*/
#ifndef _GRAPH_H_
#define _GRAPH_H_
#include <stdlib.h> 
#include "Universe.h"
typedef enum { UNDIRECTED = 0, DIRECTED } graph_type_e;

/* Adjacency list node*/
typedef struct adjlist_node
{
	int vertex;                /*Index to adjacency list array*/
	struct adjlist_node *next; /*Pointer to the next node*/
	int edge_type;             /*Indicate its edge type*/
}adjlist_node_t, *adjlist_node_p;

/* Adjacency list */
typedef struct adjlist
{
	int num_members;           /*number of members in the list (for future use)*/
	adjlist_node_t *head;      /*head of the adjacency linked list*/
}adjlist_t, *adjlist_p;
  
/* Graph structure. A graph is an array of adjacency lists.
Size of array will be number of vertices in graph*/
typedef struct graph
{
	graph_type_e type;        /*Directed or undirected graph */
	int num_vertices;         /*Number of vertices*/
	adjlist_p adjListArr;     /*Adjacency lists' array*/
}graph_t, *graph_p;

/* Exit function to handle fatal errors*/
__inline void err_exit(char* msg)
{
	printf("[Fatal Error]: %s \nExiting...\n", msg);
	exit(1);
}


struct Paths_Junction
{
	vector<vector<adjlist_node_p>> paths_part1;
	vector<vector<adjlist_node_p>> paths_part2;
};

/* Function to create an adjacency list node*/
adjlist_node_p createNode(int v, int edge_type);

/* Function to create a graph with n vertices; Creates both directed and undirected graphs*/
graph_p createGraph(int n, graph_type_e type);

/*Destroys the graph*/
void destroyGraph(graph_p graph);

/* Adds an edge to a graph*/
void addEdge(graph_t *graph, int src, int dest, int edge_type);

/* Function to print the adjacency list of graph*/
void displayGraph(graph_p graph);

//Without spatial constraints
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type);

//With spatial constraints
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[]);

//From start point to a specific end type through qualified edge type and steps
vector<vector<adjlist_node_p>> FindQualifiedPaths(graph_p graph, int start_vertex_id, int end_type, int step_num, vector<int> vector_edge_type, Entity entity_matrix[]);

//With spatial constraints,start from one type of nodes
vector<vector<adjlist_node_p>> FindQualifiedPaths_from_specific_entity_type(graph_p graph, int start_entity_type, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[]);

//With spatial constraints(allows closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type, vector<int> spatialconstraint_step_num, vector<MyRect> constraint_rect, Entity entity_matrix[]);

//From start point to a specific end through qualified edge type and steps(allow closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int start_vertex_id, int end_vertex_id, int step_num, vector<int> vector_edge_type);

//Without spatial constraints(allow closure)
vector<vector<adjlist_node_p>> FindQualifiedPaths_closure_allowed(graph_p graph, int vertex_num, int step_num, vector<int> vector_edge_type);

//Display paths on the screen using printf
void DisplayPaths(int start_vertex, vector<vector<adjlist_node_p>> vector_Paths);

//Display paths and edge type on the screen using printf
void DisplayPathsRelationship(int start_vertex, vector<vector<adjlist_node_p>> vector_Paths);

//Outfile graph to disk for storage
void GraphToDisk(graph_p graph, string filename);

//Read graph from disk to adacent list data structure
graph_p ReadGraphFromDisk(string filename);

//Outfile graph to disk for displaying
void OutFile(graph_p graph, string filename);

//Outfile paths to disk for displaying
void OutFile(int startentity_id, vector<vector<adjlist_node_p>> Paths, string filename, Entity Entity_Matrix[]);

//Generate graph with specific spatial entity ratio
graph_p Generate_Graph(int node_count, __int64 edge_count, double a, double b, double c, double nonspatial_entity_ratio);

#endif