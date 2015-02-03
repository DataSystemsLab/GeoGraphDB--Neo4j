#include "stdafx.h"
#include "Universe.h"

vector<int>hit_id;

bool MySearchCallback(int id, void* arg)
{
	//	cout << "Hit data rect " << id << "\n";
	hit_id.push_back(id);
	return true; // keep going
}

vector<int> GetHitID()
{
	return hit_id;
}

bool Location_In_Rect(Location m_location, MyRect m_rect)
{
	if (m_location.x<m_rect.left_bottom.x || m_location.x>m_rect.right_top.x || m_location.y<m_rect.left_bottom.y || m_location.y>m_rect.right_top.y)
		return false;
	else
		return true;
}

void OutFile(Entity Entity_Matrix[], int node_count, string filename)
{
	ofstream file;
	file.open("data/" + filename);
	for (int i = 0; i < node_count; i++)
	{
		file << i << "    " << Entity_Matrix[i].IsSpatial << "    " << Entity_Matrix[i].location.x << "  " << Entity_Matrix[i].location.y << "    " << Entity_Matrix[i].type;
		file << endl;
	}
	file.close();
}

void EntityToDisk(Entity Entity_Matrix[], int node_count, int range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);
	
	printf("%d %d\n", node_count,range);
	for (int i = 0; i < node_count; i++)
	{
		printf("%d %d %f %f %d\n", i, Entity_Matrix[i].IsSpatial, Entity_Matrix[i].location.x, Entity_Matrix[i].location.y, Entity_Matrix[i].type);
	}
	fclose(stdout);
}

void GenerateEntity(int node_count, Entity Entity_Matrix[], int range, double nonspatial_entity_ratio)
{
	TRnd Rnd = time(0);
	for (int i = 0; i < node_count * nonspatial_entity_ratio; i++)
	{
		Entity_Matrix[i].id = i;
		Entity_Matrix[i].IsSpatial = false;

		Entity_Matrix[i].location.x = -1;
		Entity_Matrix[i].location.y = -1;

		Entity_Matrix[i].type = (Rnd.GetUniDev()<0.1 ? 0 : 1);
	}


	for (int i = node_count * nonspatial_entity_ratio; i < node_count; i++)
	{
		Entity_Matrix[i].id = i;
		Entity_Matrix[i].IsSpatial = true;

		Entity_Matrix[i].location.x = Rnd.GetUniDev() * range;
		Entity_Matrix[i].location.y = Rnd.GetUniDev() * range;

		Entity_Matrix[i].type = (Rnd.GetUniDev()>0.5 ? 100 : 101);//100   restaurant     101    theatre
	}
}

void ReadEntityFromDisk(int &node_count, Entity Entity_Matrix[], int &range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	scanf("%d %d", &node_count, &range);
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d %lf %lf %d", &(Entity_Matrix[i].id), &(Entity_Matrix[i].IsSpatial), &(Entity_Matrix[i].location.x), &(Entity_Matrix[i].location.y), &(Entity_Matrix[i].type));
	}
	fclose(stdin);
}
