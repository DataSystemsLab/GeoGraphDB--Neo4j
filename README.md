# Spindra--GeoGraphDB

Spindra--GeoGraphDB(denoted as GeoGraphDB in the rest part) provides support for answering what we call RangeReach query which is a graph reachability query with spatial predicates.

## How to get started
1.  Download Neo4j graph database from http://neo4j.com/. If you are a linux/unix user, download the corresponding version and extract the compressed file to the where you want to locate your database. If you are a windows user, you need to download the windows version and install the software.
2.  Download GeoGraphDB source code.
3.  Construct GeoReach index and output the index as text files on disk. Code for this function is in folder "Construction". This step requires file concluding graph structure and spatial location. First line in the file to store graph structure is total number of vertices in the graph. The rest lines should be in the format of 'id,number_of_nodes_in_adacentlist,node1_id,node2_id...'. Spatial location file of eneities should organized as follows: First of spatial location file is also the number of vertices. For the rest lines, spatial entities should have the format of 'id,spatial_or_not,longtitude,latitude' while the non-spatial entities have line format of 'id,spatial_or_not'. The following gives an example for the two files. GenerateGeoReach(string graph_path, string entity_path, string GeoReach_path, int MG, double MR, int MT, Location left_bottom, Location right_top, int pieces_x, int pieces_y) is used to calculate GeoReach index. Parameters of the function can be devided into three groups. The first group is file path. It includes graph file path, entity file path and output GeoReach file path. The second is a group contains parameters used for controlling index size and query efficiency of GeoReach index. They consist of MAX_REACH_GRIDS, MAX_RMBR AND MERGE_COUNT

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
        

4.  Query processing. Following is an example to use query funtion in the package.
```java
        //...
        GeoReach p_geo = new GeoReach();
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
