Graph Processing: Find connected components in given undirected graph using GraphX (Pregel API, scala)
===============================================================================
### An undirected graph is represented in the input text file using one line per graph vertex. 
### For example, the line

1,2,3,4,5,6,7
 represents the vertex with ID 1, which is connected to the vertices with IDs 2, 3, 4, 5, 6, and 7.
######
![Undirected-Graph](p2.png?raw=true)
######
### is represented in the input file as follows:
- 3,2,1
- 2,4,3
- 1,3,4,6
- 5,6
- 6,5,7,1
- 0,8,9
- 4,2,1
- 8,0
- 9,0
- 7,6

### The purpose of this project is to write a graph-analysis program with GraphX (Pregel), that finds the connected components of any undirected graph and prints the size of these connected components. 
### A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph. For the above graph, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7. Program should print the sizes of these connected components: 3 and 7.
#####
### Pseudocode:
- Read the input graph and construct the RDD of edges
- Use the graph builder Graph.fromEdges to construct a Graph from the RDD of edges
- Access the VertexRDD and change the value of each vertex to be the vertex ID (initial group number)
- Call the Graph.pregel method in the GraphX Pregel API to find the connected components. For each vertex, this method changes its group number to the minimum group number of its neighbors (if it is less than its current group number)
- Group the graph vertices by their group number and print the group sizes


