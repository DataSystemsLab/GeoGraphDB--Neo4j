#include "stdafx.h"
#include "Universe.h"

vector<int>hit_id;
int start, runtime;

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

int GetStartTime()
{
	return start;
}

void SetStartTime(int i)
{
	start = i;
}

int GetRunTime()
{
	return runtime;
}

void SetRunTime(int i)
{
	runtime = i;
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

void OutFile(vector<Entity> entity_vector, string filename)
{
	ofstream file;
	file.open("data/" + filename);
	for (int i = 0; i < entity_vector.size(); i++)
	{
		file << i << "    " << entity_vector[i].IsSpatial << "    " << entity_vector[i].location.x << "  " << entity_vector[i].location.y << "    " << entity_vector[i].type;
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

void EntityToDIsk(vector<Entity> entity_vector, int range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "w", stdout);

	printf("%d %d\n", entity_vector.size(), range);
	for (int i = 0; i < entity_vector.size(); i++)
	{
		printf("%d %d %f %f %d\n", i, entity_vector[i].IsSpatial, entity_vector[i].location.x, entity_vector[i].location.y, entity_vector[i].type);
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

void GenerateEntity(int node_count, vector<Entity> &entity_vector, int range, double nonspatial_entity_ratio)
{
	TRnd Rnd = time(0);
	entity_vector.resize(node_count);
	//nonspatial entity
	for (int i = 0; i < node_count * nonspatial_entity_ratio; i++)
	{
		entity_vector[i].id = i;
		entity_vector[i].IsSpatial = false;

		entity_vector[i].location.x = -1;
		entity_vector[i].location.y = -1;

		//entity_vector[i].type = (Rnd.GetUniDev()<0.1 ? 0 : 1);
		double x = Rnd.GetUniDev();
		if (x < 0.5)
		{
			entity_vector[i].type = 0;
			continue;
		}
		if (x < 0.75)
		{
			entity_vector[i].type = 1;
			continue;
		}
		if (x < 0.87)
		{
			entity_vector[i].type = 2;
			continue;
		}
		if (x < 0.93)
		{
			entity_vector[i].type = 3;
			continue;
		}
		if (x < 0.96)
		{
			entity_vector[i].type = 4;
			continue;
		}
		if (x < 0.98)
		{
			entity_vector[i].type = 5;
			continue;
		}
		if (x < 0.99)
		{
			entity_vector[i].type = 6; 
			continue;
		}
		if (x < 0.995)
		{
			entity_vector[i].type = 7;
			continue;
		}
		else
			entity_vector[i].type = 8;

	}

	//spatial entity
	for (int i = node_count * nonspatial_entity_ratio; i < node_count; i++)
	{
		entity_vector[i].id = i;
		entity_vector[i].IsSpatial = true;

		entity_vector[i].location.x = Rnd.GetUniDev() * range;
		entity_vector[i].location.y = Rnd.GetUniDev() * range;

		entity_vector[i].type = (Rnd.GetUniDev()>0.5 ? 100 : 101);//100   restaurant     101    theatre
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

void ReadEntityFromDisk(int &node_count, vector<Entity> &entity_vector, int &range, string filename)
{
	string root = "data/";
	root += filename;
	char *ch = (char *)root.data();
	freopen(ch, "r", stdin);

	scanf("%d %d", &node_count, &range);
	entity_vector.resize(node_count);
	for (int i = 0; i < node_count; i++)
	{
		scanf("%d %d %lf %lf %d", &(entity_vector[i].id), &(entity_vector[i].IsSpatial), &(entity_vector[i].location.x), &(entity_vector[i].location.y), &(entity_vector[i].type));
	}
	fclose(stdin);
}

