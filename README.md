# Spindra--GeoGraphDB

Spindra--GeoGraphDB(denoted as GeoGraphDB in the rest part) provides support for answering what we call RangeReach query which is a graph reachability query with spatial predicates.

## How to get started
1.  Download Neo4j graph database from http://neo4j.com/. If you are a linux/unix user, download the corresponding version and extract the compressed file to the where you want to locate your database. If you are a windows user, you need to download the windows version and install the software.
2.  Download GeoGraphDB source code.
3.  Construct GeoReach index and load the index and graph data into neo4j database. This step requires file concluding graph structure and spatial location. First line in the file to store graph structure is total number of vertices in the graph. The rest lines should be in the format of 'id, number_of_nodes_in_adacentlist, node1_id, node2_id,...'. The spatial location file should have the format of 'id, spatial_or_not, longtitude, latitude'. The following gives an example for the two files.

<br />Graph file<br />
5<br />
0,3,1,3,4<br />
1,0<br />
2,2,3,4<br />
3,1,4<br />
4,0<br />

Spatial location file<br />
0,1,23.334,55.2442<br />
1,0,0,0<br />
2,0,0,0<br />
3,1,110.234,-30.3234<br />
4,1,-93.436427,48.4165<br />

Function Construct(graph_file_path, location_file_path, neo4j_db_path) is used for such step. Parameters are file paths of all required files and database. 



## Contact

### Contributors
* [Yuhan Sun] (Email: yuhan.sun.1@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

###Project website

### DataSys Lab
GeoSpark is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).
