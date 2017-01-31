# Spindra--GeoGraphDB

Spindra--GeoGraphDB(denoted as GeoGraphDB in the rest part) provides support for answering what we call RangeReach query which is a graph reachability query with spatial predicates.

## How to get started
1.  Download Neo4j graph database from http://neo4j.com/. If you are a linux/unix user, download the corresponding version and extract the compressed file to the where you want to locate your database. If you are a windows user, you need to download the windows version and install the software.
2.  Download GeoGraphDB source code.
3.  Construct GeoReach index and output the index as text files on disk. Code for this function is in folder "Construction". This step requires two text files, including file for graph structure and for entities with spatial locations. First line in the graph file is total number of vertices in the graph. The rest lines should be in the format of 'id,number_of_nodes_in_adacentlist,node1_id,node2_id...'. Spatial location file of eneities should organized as follows: First of spatial location file is also the number of vertices. For the rest lines, entities should have the format of 'id,spatial_or_not,longtitude,latitude' while the non-spatial entities have line format of 'id,spatial_or_not'. The following gives an example for the two files. 

        Graph file
        5
        0,3,1,3,4
        1,0
        2,2,3,4
        3,1,4
        4,0

        Spatial location file
        5
        0,1,23.334,55.2442
        1,0
        2,0
        3,1,110.234,-30.3234
        4,1,-93.436427,48.4165

GenerateGeoReach(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y) function is used to calculate GeoReach index. Parameters of the function can be devided into three groups. The first group is file path. It includes graph file path, entity file path and output GeoReach file path. The second is a group contains parameters used for controlling index size and query efficiency of GeoReach index. They consist of MAX_REACH_GRIDS, MAX_RMBR AND MERGE_COUNT ( MG, MR and MT respectively). 

        

4. Load data into neo4j database. This section is implemented by using java. The function is in LoadData folder. Paremeters of construction function of Batch_Inserter include three data source file paths and a neo4j database file path where the algorithm is going to load all the graph data and GeoReach index data. There are existing files that can be tested in test_data folder.
```java
        String graph_path = "Documents/GeoReach/graph.txt";
        String entity_path = "Documents/GeoReach/entity.txt";
        String GeoReach_path = "DOcuments/GeoReach/GeoReach.txt";
        String db_path = "Documents/GeoReach/neo4j-community-2.3.3/data/graph.db";
        Batch_Inserter batch_inserter = new Batch_Inserter(graph_path, entity_path, GeoReach_path, db_path);
```
5. Query processing. Following is an example to use query funtion in the package.
```java
        //...
        int split_pieces = 128;
        MyRectangle spatial_range = (-180,-90,180,90);
        GeoReach p_geo = new GeoReach(spatial_range,split_pieces);
        int id = 356;
        MyRectangle query_rect = new MyRectangle(-30,10,60,40);
        boolean result = p_geo.ReachabilityQuery(id,query_rect);
```

## Contact

### Contributors
* [Yuhan Sun] (Email: yuhan.sun.1@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

###Project website

### DataSys Lab
GeoSpark is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).
