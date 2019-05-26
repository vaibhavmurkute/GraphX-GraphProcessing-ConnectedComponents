import org.apache.spark.graphx.{Graph, VertexId, Edge, EdgeDirection}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD

/*
	Project: GraphX, Pregel API-GraphProcessing- find Connected Components
	Author: Vaibhav Murkute
*/

object Graph {
  def main ( args: Array[String] ) {
	val conf = new SparkConf().setAppName("ConnectedComponents");
	val sc = new SparkContext(conf);
	
	//val graph: Graph[Long, Long]  = GraphLoader.edgeListFile(sc, args(0));
	
	val file = sc.textFile(args(0));
	val edge_rdd = file.map( line => { val (vertex, adj_list) = line.split(",").splitAt(1)
		(vertex(0).toLong, adj_list.toList.map(_.toLong));

	}).flatMap(vertices => vertices._2.map(vert => (vertices._1, vert)))
		.map(v => Edge(v._1, v._2, v._1))

	var initial_graph = org.apache.spark.graphx.Graph.fromEdges(edge_rdd, "defaultProperty")

	// map each vertex to its vertex id
	val graph: org.apache.spark.graphx.Graph[Long, Long] = initial_graph.mapVertices((vert_id, _) => vert_id)
	
	val result = graph.pregel(Long.MaxValue, 5, EdgeDirection.Either) (
		 // vertex program
		(id, grp, new_grp) => math.min(grp, new_grp),
		// send message function
		triplet => {
			if(triplet.attr < triplet.dstAttr){
				Iterator((triplet.dstId, triplet.attr))
			}else if(triplet.srcAttr < triplet.attr){
				Iterator((triplet.dstId, triplet.srcAttr))
			}else{
				Iterator.empty;
			}
		},
		// Merge Message Function
		(a,b) => math.min(a,b)
	)

	result.vertices.map(graph => (graph._2, 1)).reduceByKey(_ + _).sortByKey().collect().foreach(println)

  }
}
 
