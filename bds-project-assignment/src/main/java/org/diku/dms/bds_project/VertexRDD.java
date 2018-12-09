package org.diku.dms.bds_project;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

/**
 * An RDD that exposes the interfaces as storing a collection of vertices (a
 * tuple of vertex id and vertex attribute). The data are actually stored as a
 * collection of `VertexPartition` instances.
 *
 */
@SuppressWarnings("serial")
public class VertexRDD<VD> extends RDD<Tuple2<VertexId, VD>> {
	public final JavaRDD<VertexPartition<VD>> partitionsRDD;

	/**
	 * Constructor method, which calls the super class's constructor method first.
	 * `Tuple2.class` implies it exposes the interfaces as a collection of `Tuple2`
	 * instances. The function `rdd.iterator(part, context)` returns an iterator, we
	 * call `next()` to get the `VertexPartitition` instance because there is only a
	 * single `VertexPartition` instance in that iterator.
	 * 
	 * @param partitionsRDD a JavaRDD consisting of a collection of
	 *                      `VertexPartition` instances.
	 */
	public VertexRDD(JavaRDD<VertexPartition<VD>> partitionsRDD) {
		super(partitionsRDD.rdd(), scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
		this.partitionsRDD = partitionsRDD; // keep the reference of the parent RDD instance for later use.
	}

	/**
	 * Abstract method inherited from RDD class. Given a partition `part` of this
	 * `VertexRDD` instance, return an iterator of `Tuple2` instances which present
	 * the vertices stored in `part`.
	 * 
	 */
	@Override
	public scala.collection.Iterator<Tuple2<VertexId, VD>> compute(Partition part, TaskContext context) {
		@SuppressWarnings("unchecked")
		VertexPartition<VD> partition = (VertexPartition<VD>) this
				.firstParent(scala.reflect.ClassTag$.MODULE$.apply(VertexPartition.class)).iterator(part, context)
				.next();
		return partition.iterator();
	}

	/**
	 * Abstract method inherited from RDD class. Return all partitions contained in
	 * this `VertexPartition` instance.
	 */
	@Override
	public Partition[] getPartitions() {
		return this.firstParent(scala.reflect.ClassTag$.MODULE$.apply(VertexPartition.class)).getPartitions();
	}

	/**
	 * Convert a JavaRDD that consists of vertices to a new `VertexRDD`, and create
	 * routing table for each partition based on `EdgeRDD`.
	 * 
	 * @param          <VD> vertex attribute data type
	 * @param          <ED> edge attribute data type
	 * @param vertices a JavaRDD consists of a collection of vertices
	 * @param edges    a `EdgeRDD` instance which consists of a collection of edges
	 * @return a new `VertexRDD`
	 */
	@SuppressWarnings("unchecked")
	public static <VD, ED> VertexRDD<VD> fromVerticesAndEdgeRDD(JavaPairRDD<VertexId, VD> vertices, EdgeRDD<ED> edges) {
		
		 // to ensure `vertices` is partitioned.
		JavaPairRDD<VertexId, VD> partitionedVertices = vertices.partitioner().isPresent() ? vertices
				: vertices.partitionBy(new HashPartitioner(vertices.partitions().size()));

		// create routing table from `edges`, and partition it by the same partitioner as used by `partitionedVertices`
		JavaRDD<RoutingTablePartition> routingTable = createRoutingTables(edges,
				partitionedVertices.partitioner().get()); 

		@SuppressWarnings("rawtypes")
		FlatMapFunction2 f = new FlatMapFunction2<
									Iterator<Tuple2<VertexId, VD>>, 
									Iterator<RoutingTablePartition>, 
									VertexPartition>() {

			@Override
			public Iterator<VertexPartition> call(Iterator<Tuple2<VertexId, VD>> vertexIter,
				Iterator<RoutingTablePartition> routingTableIter) throws Exception {
			
				// there is only one instance of RoutingTablePartition in the iterator
				RoutingTablePartition routingTable = routingTableIter.next();
			
				// create a new instance of ShippableVertexPartition using the vertices and routing table contained in the same partition.
				return ShippableVertexPartition.fromVerticesAndRoutingTable(vertexIter, routingTable);
			}
		};

		// Combine the routing information  with vertices using one-to-one partition mapping.
		JavaRDD<VertexPartition<VD>> vertexPartitions = partitionedVertices.zipPartitions(routingTable, f); 
		return fromVertexPartitions(vertexPartitions);
	}

	/**
	 * Given an `EdgeRDD`, generate routing table for each `VertexPartition`. For
	 * example, vertices 1, 2, 3 contained in edge partition 1, and vertices 2, 3, 4
	 * contained in edge partition 2. In each edge partition, vertex-partition id
	 * pairs are generated as:
	 * 
	 * vertex id, partition id 1, 1 2, 1 3, 1 2, 2 3, 2 4, 2
	 * 
	 * The above key-value pairs will be partitioned by the same partitioner as
	 * vertices RDD. For example, id 1, 3 are in partition 1 and id 2, 4 are in
	 * partition 2. The above key-value pairs will be partitioned as follows.
	 * 
	 * Partition 1 1, 1 3, 1 3, 2 ---------- Partition 2 2, 1 2, 2 4, 2
	 * 
	 * Then these key-value pairs will be aggregated on partition id as: Partition 1
	 * [1, 3]->1 [3] -> 2 ------------ Partition 2 [2] -> 1 [2, 4] -> 2
	 * 
	 * The above data will be converted to routing table further, forming to
	 * `RoutingTablePartition` instances.
	 * 
	 * @param                     <VD> vertex attribute data type
	 * @param                     <ED> edge attribute data type
	 * @param edges               an `EdgeRDD` used for creating routing table
	 * @param verticesPartitioner the partitioner used by the vertices RDD
	 * @return a JavaRDD consists of `RoutingTablePartition` instances
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static <VD, ED> JavaRDD<RoutingTablePartition> createRoutingTables(EdgeRDD<ED> edges,
			Partitioner verticesPartitioner) {
		PairFlatMapFunction f = new PairFlatMapFunction<Iterator<Tuple2<PartitionId, EdgePartition<ED>>>, VertexId, PartitionId>() {

			@Override
			public java.util.Iterator<Tuple2<VertexId, PartitionId>> call(
					Iterator<Tuple2<PartitionId, EdgePartition<ED>>> iter) throws Exception {
				
				// take the only one EdgePartition instance from the iterator.
				Tuple2<PartitionId, EdgePartition<ED>> tuple = iter.next(); 
				
				// call the function to generate a list of VertexId-PartitionId pairs.
				return RoutingTablePartition.routingInformationFromEdgePartition(tuple._1, tuple._2); 
			}
		};

		// map each vertex contained by this edge partition to VertexId-PartitionId pair.
		JavaPairRDD<VertexId, PartitionId> vid2pid = edges.partitionsRDD.mapPartitionsToPair(f); 

		int numEdgePartition = edges.partitions().length;
		FlatMapFunction f2 = new FlatMapFunction<Iterator<Tuple2<VertexId, PartitionId>>, RoutingTablePartition>() {

			@Override
			public Iterator<RoutingTablePartition> call(Iterator<Tuple2<VertexId, PartitionId>> iter) throws Exception {
				// create the  new instance of RoutingTablePartition as an iterator.
				return Arrays.asList(RoutingTablePartition.fromRoutingInformation(numEdgePartition, iter)).iterator(); 
			}
		};
		
		 // Partition routing information using the same partitioner as vertices, and then map each partition of it to a RoutingTableParition.
		return vid2pid.partitionBy(verticesPartitioner).mapPartitions(f2);
	}

	/**
	 * Given a JavaRDD consists of a collection of `VertexPartition` instances,
	 * convert it to a new `VertexRDD` instance.
	 * 
	 * @param VD               vertex attribute data type
	 * @param vertexPartitions a JavaRDD consists of a collection of
	 *                         `VertexPartition` instances
	 * @return a new `VertexRDD`
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static <VD> VertexRDD<VD> fromVertexPartitions(JavaRDD<VertexPartition<VD>> vertexPartitions) {
		return new VertexRDD(vertexPartitions);
	}

	/**
	 * 
	 * @return the number of vertices contained in `VertexRDD`
	 */
	public long numVertices() {
		return partitionsRDD.map(vp -> vp.numVertices()) // map each VertexPartition instances to the number of vertices
															// it contains.
				.reduce((a, b) -> a + b); // sum up the number of vertices of all VertexPartition instances.
	}
}
