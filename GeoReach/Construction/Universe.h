/*Universe.h*/
#ifndef _UNIVERSE_H_
#define _UNIVERSE_H_
#include <iostream>
#include <fstream>
using namespace std;
#include <vector>
#include <set>
#include <hash_set>
#include <hash_map>
#include <stack>
#include <queue>
#include <iterator>
#include <time.h>
#include <sstream>

/*General utilization functions*/
string getstring(const int i);
int StringtoInt(string str);
vector<string> split(string str, string pattern);

/*Point location*/
struct Location
{
	double x, y;

	Location(double a, double b)
	{
		x = a;
		y = b;
	}
	Location()
	{
		x = 0;
		y = 0;
	}

	Location copy()
	{
		Location p_location = Location(x, y);
		return p_location;
	}
};

/*Rectangle*/
struct MyRect
{
	//whether it has a real rectangle or just a empty rectangle
	boolean HasRec;

	Location left_bottom;
	Location right_top;

	MyRect(double minx, double miny, double maxx, double maxy)
	{
		HasRec = true;
		left_bottom.x = minx;
		left_bottom.y = miny;
		right_top.x = maxx;
		right_top.y = maxy;
	}

	MyRect(Location p_left_bottom, Location p_right_top)
	{
		HasRec = true;
		left_bottom = Location(p_left_bottom.x, p_left_bottom.y);
		right_top = Location(p_right_top.x, p_right_top.y);
	}

	MyRect()
	{
		HasRec = false;
		left_bottom.x = 0;
		left_bottom.y = 0;
		right_top.x = 0;
		right_top.y = 0;
	}

	MyRect copy()
	{
		MyRect rect = MyRect(left_bottom, right_top);
		return rect;
	}

	void MBR(MyRect end_rect)
	{
		//end_rect is null
		if (!end_rect.HasRec)
			return;
		else
		{
			//current is null
			if (!HasRec)
			{
				HasRec = true;
				left_bottom = end_rect.left_bottom.copy();
				right_top = end_rect.right_top.copy();
			}
			else
			{
				if (left_bottom.x > end_rect.left_bottom.x)
					left_bottom.x = end_rect.left_bottom.x;
				if (left_bottom.y > end_rect.left_bottom.y)
					left_bottom.y = end_rect.left_bottom.y;
				if (right_top.x < end_rect.right_top.x)
					right_top.x = end_rect.right_top.x;
				if (right_top.y < end_rect.right_top.y)
					right_top.y = end_rect.right_top.y;
			}
		}
	}

	void MBR(Location point)
	{
		if (!HasRec)
		{
			HasRec = true;
			left_bottom = Location(point.x, point.y);
			right_top = Location(point.x, point.y);
		}
		else
		{
			if (left_bottom.x > point.x)
				left_bottom.x = point.x;
			if (left_bottom.y > point.y)
				left_bottom.y = point.y;
			if (right_top.x < point.x)
				right_top.x = point.x;
			if (right_top.y < point.y)
				right_top.y = point.y;
		}
	}

	double Area()
	{
		if (!HasRec)
			return 0;
		else
			return (right_top.x - left_bottom.x)*(right_top.y - left_bottom.y);
	}
};

/*Vertex Entity*/
struct Entity
{
	int id;
	bool IsSpatial;
	Location location;
	MyRect RMBR;
	int type;
	int scc_id;
};

//Read entity from disk storage
void ReadEntityInSCCFromDisk(int &node_count, vector<Entity> &entity_vector, int &range, string filename);

//Read entity
void ReadEntity(int &node_count, vector<Entity> &entity_vector, string filename);

/*Graph related functions*/
//Topological sort of dag
void TopologicalSortUtil(int v, vector<bool> &visited, queue<int> &Queue, vector<vector<int>> &graph);
void TopologicalSort(vector<vector<int>> &graph, queue<int> &Queue);

/*Read Graph from adjacent list txt file*/
void ReadGraph(vector<vector<int>> &graph, int &node_count, string graph_filepath);

/*Get in_edge graph*/
void GenerateInedgeGraph(vector<vector<int>> &graph, vector<vector<int>> &in_edge_graph);

#endif