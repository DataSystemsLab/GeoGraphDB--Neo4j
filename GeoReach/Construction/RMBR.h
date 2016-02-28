/*RMBR.h*/
#ifndef _RMBR_H_
#define _RMBR_H_

#include "Universe.h"


bool RecUnion(MyRect &rect_v, MyRect &rect_neighbour);
//RMBR index generation with topological sorting queue
void GenerateRMBR(vector<Entity> &p_entity, vector<vector<int>> &graph, queue<int>& queue, vector<MyRect> &RMBR);

void RMBR_To_Disk(vector<MyRect> &RMBR, string filename);
void ReadRMBR(vector<MyRect> &RMBR, string filename);

#endif