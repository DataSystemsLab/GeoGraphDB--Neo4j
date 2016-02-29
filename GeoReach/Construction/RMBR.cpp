#include "stdafx.h"
#include "RMBR.h"

bool RecUnion(MyRect &rect_v, MyRect &rect_neighbour)
{
	bool flag = false;

	//Either of them has RMBR
	if (!rect_v.HasRec && !rect_neighbour.HasRec)
		return false;

	/*At least one has RMBR*/

	//v has no RMBR but neighbor has
	if (!rect_v.HasRec)
	{
		rect_v.left_bottom.x = rect_neighbour.left_bottom.x;
		rect_v.left_bottom.y = rect_neighbour.left_bottom.y;
		rect_v.right_top.x = rect_neighbour.right_top.x;
		rect_v.right_top.y = rect_neighbour.right_top.y;
		rect_v.HasRec = true;
		return true;
	}

	//v has RMBR but neighbor does not
	if (!rect_neighbour.HasRec)
		return false;

	//Both of them have RMBR
	if (rect_neighbour.left_bottom.x < rect_v.left_bottom.x)
	{
		rect_v.left_bottom.x = rect_neighbour.left_bottom.x;
		flag = true;
	}
	if (rect_neighbour.left_bottom.y < rect_v.left_bottom.y)
	{
		rect_v.left_bottom.y = rect_neighbour.left_bottom.y;
		flag = true;
	}

	if (rect_neighbour.right_top.x > rect_v.right_top.x)
	{
		rect_v.right_top.x = rect_neighbour.right_top.x;
		flag = true;
	}
	if (rect_neighbour.right_top.y > rect_v.right_top.y)
	{
		rect_v.right_top.y = rect_neighbour.right_top.y;
		flag = true;
	}
	return flag;
}

void UpdateRMBR(int start_id, int end_id, vector<Entity> &p_entity, vector<MyRect> &RMBR)
{
	RMBR[start_id].MBR(RMBR[end_id]);
	if (p_entity[end_id].IsSpatial)
		RMBR[start_id].MBR(p_entity[end_id].location);
}

void GenerateRMBR(vector<Entity> &p_entity, vector<vector<int>> &graph, queue<int>& queue, vector<MyRect> &RMBR)
{
	while (!queue.empty())
	{
		int start_id = queue.front();
		queue.pop();

		for (int i = 0; i < graph[start_id].size(); i++)
		{
			int end_id = graph[start_id][i];
			UpdateRMBR(start_id, end_id, p_entity, RMBR);
		}
	}
}


//void GenerateRMBR(vector<Entity> &p_entity, vector<vector<int>> &in_edge_graph, queue<int>& queue, vector<MyRect> &RMBR)
//{
//	while (!queue.empty())
//	{
//		int id = queue.front();
//		queue.pop();
//
//		if (p_entity[id].IsSpatial)
//		{
//			MyRect rec = MyRect(p_entity[id].location.x, p_entity[id].location.y, p_entity[id].location.x, p_entity[id].location.y);
//			for (int i = 0; i < in_edge_graph[id].size(); i++)
//				RecUnion(RMBR[in_edge_graph[id][i]], rec);
//		}
//		for (int i = 0; i < in_edge_graph[id].size(); i++)
//			RecUnion(RMBR[in_edge_graph[id][i]], RMBR[id]);
//	}
//}

void RMBR_To_Disk(vector<MyRect> &RMBR, string filename)
{
	char *ch = (char *)filename.data();
	freopen(ch, "w", stdout);

	printf("%d\n", RMBR.size());
	for (int i = 0; i < RMBR.size(); i++)
	{
		printf("%d %d ", i, RMBR[i].HasRec);
		if (RMBR[i].HasRec)
			printf("%f %f %f %f",RMBR[i].left_bottom.x, RMBR[i].left_bottom.y, RMBR[i].right_top.x, RMBR[i].right_top.y);
		printf("\n");
	}
	fclose(stdout);
}

void ReadRMBR(vector<MyRect> &RMBR, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	int node_count = 0;

	scanf("%d", &node_count);
	RMBR.resize(node_count);
	
	int id;
	int HasRMBR;
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d ", &id, &HasRMBR);
		if (HasRMBR == 0)
		{
			RMBR[id] ;
			continue;
		}
		else
			scanf("%lf %lf %lf %lf", &(RMBR[id].left_bottom.x), &(RMBR[id].left_bottom.y), &(RMBR[id].right_top.x), &(RMBR[id].right_top.y));
	}
	fclose(stdin);
}


