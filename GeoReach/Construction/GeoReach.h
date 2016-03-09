/*Universe.h*/
#ifndef _GEOREACH_H_
#define _GEOREACH_H_

#include "RMBR.h"
#include "ReachGrid.h"

void GenerateRMBR(string graph_path, string entity_path, string RMBR_path);

int InitializeType(vector<vector<int>> &graph, vector<int> &Types, int start_id, vector<bool> &GeoB);

void GenerateGeoReach(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom = Location(-180, -90), Location right_top = Location(180, 90), int pieces_x = 128, int pieces_y = 128);
void GenerateGeoReachFromInedgeGraph(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y);

void GeoReachToDisk(string GeoReach_path, vector<int> &Types, vector<vector<bool>> &ReachGrid, vector<MyRect> &RMBR, vector<bool> &GeoB);
#endif