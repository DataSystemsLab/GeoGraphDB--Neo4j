/*Universe.h*/
#ifndef _UNIVERSE_H_
#define _UNIVERSE_H_
#include <fstream>
using namespace std;
#include <vector>
#include <stack>
#include "Rand.h"
#include "RTree.h"

struct Location
{
	double x, y;
};

struct MyRect
{
	Location left_bottom;
	Location right_top;
};

struct Entity
{
	int id;
	bool IsSpatial;
	Location location;
	int type;
};

struct Rect
{
	Rect()  {}

	Rect(double a_minX, double a_minY, double a_maxX, double a_maxY)
	{
		min[0] = a_minX;
		min[1] = a_minY;

		max[0] = a_maxX;
		max[1] = a_maxY;
	}


	double min[2];
	double max[2];
};

struct edge
{
	int edge_type;
	int vertex;
};

bool MySearchCallback(int id, void* arg);

vector<int> GetHitID();

//Judge whether a location is in a specific rectangle
bool Location_In_Rect(Location m_location, MyRect m_rect);

//Outfile entity to disk for storage
void EntityToDisk(Entity Entity_Matrix[], int node_count, int range, string filename);

//Out file entity to disk for displaying
void OutFile(Entity Entity_Matrix[], int node_count, string filename);

//Generate entity with specific spatial entity ratio
void GenerateEntity(int node_count, Entity Entity_Matrix[], int range, double nonspatial_entity_ratio);

//Read entity from disk storage
void ReadEntityFromDisk(int &node_count, Entity Entity_Matrix[], int &range,string filename);

#endif