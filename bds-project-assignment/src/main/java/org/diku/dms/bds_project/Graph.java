package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.diku.dms.bds_project.query.EdgePattern;
import org.diku.dms.bds_project.query.MatchesRDD;
import org.diku.dms.bds_project.query.PatternGraph;

import scala.Function2;
import scala.Tuple2;

/**
 * Graph consists of three RDDs: `VertexRDD`, `EdgeRDD` and `EdgeTripletRDD`.
 * 
 * `vertices` contains a collection of vertices.
 * `edges` contains a collection of edges.
 * `edgeTriplets` is something that combines `vertices` and `edges`, which has vertex attributes on edges.
 *
 * @param <VD> vertex attribute data type
 * @param <ED> edge attribute data type
 */
@SuppressWarnings("serial")
public class Graph<VD, ED> implements Serializable {
	public VertexRDD<VD> vertices;
	public EdgeRDD<ED> edges;
	public EdgeTripletRDD<ED, VD> edgeTriplets;
	
	SparkConf conf = new SparkConf().setAppName("EdgePartition").setMaster("local[2]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	/**
	 * 
	 * @param vertices a collection of vertices
	 * @param edges a collection of edges
	 */
	public Graph(VertexRDD<VD> vertices, EdgeRDD<ED> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public static <VD, ED> Graph<VD, ED> fromEdgesAndVetices(JavaRDD<Edge<ED>> edges,
			JavaPairRDD<VertexId, VD> vertices) {
		EdgeRDD<ED> edgeRDD = EdgeRDD.fromEdges(edges);
		VertexRDD<VD> vertexRDD = VertexRDD.fromVerticesAndEdgeRDD(vertices, edgeRDD);
		return new Graph<VD, ED>(vertexRDD, edgeRDD);
	}
	
	/**
	 * The example of how to count the number of edges contained in the graph.
	 * see EdgeRDD.numEdges() and EdgePartition.numEdges()
	 * @return the number of edges contained in this graph
	 */
	public long numEdges() { 
		return edges.numEdges();
	}

	/**
	 * The example of how to count the number of vertices contained in the graph.
	 * see VertexRDD.numVertices() and VertexPartition.numVertices()
	 * 
	 * @return the number of vertices contained in this graph
	 */
	public long numVertices() {  
		return vertices.numVertices();
	}
	
	/**
	 * Ship vertex attributes from `vertices` (according to `routingTable` in each `VertexPartition`) to `edges`, obtaining the new `EdgeTripletRDD`. 
	 */
	public EdgeTripletRDD<ED,VD> shipVertexAttributes() {
		// implemented
		List<VertexPartition<VD>> verticePartitions= vertices.partitionsRDD.collect();
		List<Tuple2<PartitionId, EdgePartition<ED>>> edgePartitions=edges.partitionsRDD.collect();
		
		Iterator<VertexPartition<VD>> verItr=verticePartitions.iterator();
		Iterator<Tuple2<PartitionId, EdgePartition<ED>>> edgeItr=edgePartitions.iterator();
		
		List<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetriplets=
				new ArrayList<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>>();
		
		while(edgeItr.hasNext()){
			Tuple2<PartitionId, EdgePartition<ED>> tuple=edgeItr.next();
			while(verItr.hasNext()) {
				VertexPartition<VD> vertexPartions=verItr.next();
				scala.collection.Iterator<Tuple2<VertexId, VD>> vertices= vertexPartions.iterator();
				EdgePartition<ED> edgePartition=tuple._2;
				PartitionId pid=tuple._1;
				
				EdgeTripletPartition<ED,VD> partitionedEdgetriplets=
						EdgeTripletPartition.fromEdgePartitionAndVertices(edgePartition, vertices);
				
				Tuple2<PartitionId, EdgeTripletPartition<ED,VD>> element=new Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>(pid,partitionedEdgetriplets);
				edgetriplets.add(element);
			}
		}
		
		JavaRDD<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletPartitions=sc.parallelize(edgetriplets);
		return EdgeTripletRDD.fromEdgeTripletPartitions(edgetripletPartitions);
	}
	
	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with indegree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> inDegrees() {
		return edges.inDegrees();
	}
	
	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with outdegree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> outDegrees() {
		return edges.outDegrees();
	}

	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with total degree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> degrees() {
		return edges.degrees();
	}
	
	/**
	 * Given a edge pattern, return `MatchesRDD` which contains a collection of vertex id pairs, which has the edge triplet can match this edge pattern.
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in this graph try to match
	 * @return a new `MatchesRDD`
	 */
	@SuppressWarnings("rawtypes")
	public MatchesRDD matchEdgePattern(EdgePattern edgePattern) {
		return new MatchesRDD(edgeTriplets.matchEdgePattern(edgePattern));
	}
	
	/**
	 * Given a pattern graph, return the matches of subgraph contained in this graph as `MatchesRDD`.
	 * 
	 * @param patternGraph
	 * @return a new `MatchesRDD`
	 */
	@SuppressWarnings("rawtypes")
	public MatchesRDD match(PatternGraph patternGraph) {
		// Please implement this function.
		return null;
	}
	
	//implemented
	public JavaRDD<Tuple2<VertexId, VertexId>> getAllEdgesByLabels(VD vertexLabel, ED edgeLabel, boolean edgeDirection){
		List<Tuple2<VertexId, VertexId>> alledgesbylabels=new ArrayList<Tuple2<VertexId, VertexId>>();
		
		List<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgeTripletPartitions=edgeTriplets.partitionsRDD.collect();
		Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletItr=edgeTripletPartitions.iterator();
		
		while(edgetripletItr.hasNext()) {
			EdgeTripletPartition<ED,VD> edgetripletpartition = edgetripletItr.next()._2;
			List<Integer> edgepos = new ArrayList<Integer>();
			int i = 0;
			for (ED edgelabel : edgetripletpartition.data)
				if (edgelabel == edgeLabel) edgepos.add(i++);
			
			Iterator<Integer> posItr = edgepos.iterator();
			while (posItr.hasNext()) {
				VertexId src = edgetripletpartition.local2global[edgetripletpartition.localSrcIds[posItr.next()]];
				VertexId dst = edgetripletpartition.local2global[edgetripletpartition.localDstIds[posItr.next()]];
				if (edgeDirection) {// check source vertex
					if (vertexLabel == edgetripletItr.next()._2.getVertexAttrs(posItr.next()))
						alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst));
				} else {// check destination vertex
					if (vertexLabel ==  edgetripletItr.next()._2.getVertexAttrs(posItr.next()))
						alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst));
				}
			}
		}
		return sc.parallelize(alledgesbylabels); 
		
	}
	
	//implemented
	public JavaRDD<Tuple2<VertexId,VertexId>> getEdgesFromVertexByLabels(VertexId vertexId, ED edgeLabel, boolean edgeDirection){
		List<Tuple2<VertexId, VertexId>> alledgesbylabels=new ArrayList<Tuple2<VertexId, VertexId>>();
		
		List<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgeTripletPartitions=edgeTriplets.partitionsRDD.collect();
		Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletItr=edgeTripletPartitions.iterator();
		
		while(edgetripletItr.hasNext()) {
			EdgeTripletPartition<ED,VD> edgetripletpartition = edgetripletItr.next()._2;
			List<Integer> edgepos = new ArrayList<Integer>();
			int i = 0;
			for (ED edgelabel : edgetripletpartition.data)
				if (edgelabel == edgeLabel)
					edgepos.add(i++);
			Iterator<Integer> posItr = edgepos.iterator();

			while (posItr.hasNext()) {
				VertexId src = edgetripletpartition.local2global[edgetripletpartition.localSrcIds[posItr.next()]];
				VertexId dst = edgetripletpartition.local2global[edgetripletpartition.localDstIds[posItr.next()]];
				if (edgeDirection) {// check source vertex
					if (vertexId == src)
						alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst));
				} else {// check destination vertex
					if (vertexId == dst)
						alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst));
				}
			}
		}
		
		return sc.parallelize(alledgesbylabels);
	}
}
