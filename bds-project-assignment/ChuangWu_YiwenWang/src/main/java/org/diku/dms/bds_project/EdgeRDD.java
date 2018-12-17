package org.diku.dms.bds_project;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

/**
 * An RDD that exposes the interfaces to others as storing a collection of
 * edges. The data are actually stored as a collection of `EdgeParitition`
 * instances.
 *
 * @param <ED> the edge attribute data type
 */
@SuppressWarnings("serial")
public class EdgeRDD<ED> extends RDD<Edge<ED>> {
	public final JavaRDD<Tuple2<PartitionId, EdgePartition<ED>>> partitionsRDD;
	
	/**
	 * In the constructor method, we need to call super class constructor function
	 * first. `Edge.class` implies it exposes the interfaces to others as storing a
	 * collection of edges.
	 * 
	 * @param partitionsRDD an JavaRDD consisting of a collection of tuples, which
	 *                      has an `EdgePartition` instance and the corresponding
	 *                      partition id.
	 */
	public EdgeRDD(JavaRDD<Tuple2<PartitionId, EdgePartition<ED>>> partitionsRDD) {
		super(partitionsRDD.rdd(), scala.reflect.ClassTag$.MODULE$.apply(Edge.class));
		this.partitionsRDD = partitionsRDD; // keep the reference of parent `RDD` for later use
	}

	/**
	 * Abstract method inherited from RDD class. Given a partition `part` of this
	 * `EdgeRDD` instance, return an iterator of edges contained in it. The function
	 * `rdd.iterator(part, context)` returns an iterator, we call `next()` to get
	 * the `EdgePartitition` instance because there is only a single `EdgePartition`
	 * instance in that iterator.
	 */
	@SuppressWarnings("unchecked")
	public scala.collection.Iterator<Edge<ED>> compute(Partition part, TaskContext context) {
		Tuple2<PartitionId, EdgePartition<ED>> tuple = (Tuple2<PartitionId, EdgePartition<ED>>) this
				.firstParent(scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).iterator(part, context).next();
		return tuple._2.iterator();
	}

	/**
	 * Abstract method inherited from RDD class. Return all partitions contained by
	 * this `EdgeRDD`.
	 */
	public Partition[] getPartitions() {
		return this.firstParent(scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).partitions();
	}

	/**
	 * 
	 * @return the number of edges contained by `EdgeRDD`
	 */
	public long numEdges() {
		// `tuple` is of type Tuple2<PartitionId, EdgePartition>, so it maps each tuple to 
		//the number of edges in its EdgePartition instance.
		// reduce() function sum up number of edges of all VertexPartition instances.
		return partitionsRDD.map(tuple -> tuple._2.numEdges()).reduce((a, b) -> a + b); 
	}

	/**
	 * Create a new `EdgeRDD` based on `JavaRDD` consisting of a collection of
	 * edges.
	 * 
	 * @param       <ED> edge attribute data type
	 * @param edges a collection of edges as an `JavaRDD`
	 * @return return a new `EdgeRDD`
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <ED> EdgeRDD<ED> fromEdges(JavaRDD<Edge<ED>> edges) {

		// The function receives a partition id and an iterator of vertices and returns
		// an EdgePartition instance.
		Function2 f = new Function2<Integer, Iterator<Edge<ED>>, Iterator<Tuple2<PartitionId, EdgePartition>>>() {

			@Override
			public Iterator<Tuple2<PartitionId, EdgePartition>> call(Integer pid, Iterator<Edge<ED>> edges)
					throws Exception {
				// create a new `EdgePartitionBuilder`
				EdgePartitionBuilder<ED> builder = new EdgePartitionBuilder<ED>();

				// add each edge contained by this partition to the builder
				while (edges.hasNext()) {
					Edge<ED> e = edges.next();
					builder.add(e.srcId, e.dstId, e.attr);
				}

				// Build the EdgePartition instance from builder and pair up with the partition id.
				Tuple2<PartitionId, EdgePartition> tuple = new Tuple2(new PartitionId(pid), builder.toEdgePartition());

				return Arrays.asList(tuple).iterator();
			}

		};

		// Call mapPartitionsWithIndex() to operate on the partition id and the
		// collection of edges contained by this partition.
		JavaRDD<Tuple2<PartitionId, EdgePartition<ED>>> edgePartitions = edges.mapPartitionsWithIndex(f, true);
		return EdgeRDD.fromEdgePartitions(edgePartitions);
	}

	/**
	 * Create a new `EdgeRDD` from `JavaRDD` that consists of a collection of tuples
	 * which has an `EdgePartition` instance and the corresponding partition id.
	 * 
	 * @param edgePartitions an JavaRDD consisting of pairs of partition id and
	 *                       `EdgePartition`.
	 * @return a new `EdgeRDD`
	 */
	private static <ED> EdgeRDD<ED> fromEdgePartitions(JavaRDD<Tuple2<PartitionId, EdgePartition<ED>>> edgePartitions) {
		return new EdgeRDD<ED>(edgePartitions);
	}

	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represents a
	 *         vertex id with the indegree of that vertex
	 */
	public JavaRDD<Tuple2<VertexId, Long>> inDegrees() {
		//implemented
		return calDegrees(EdgeDirection.IN);
	}

	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represents a
	 *         vertex id with the out-degree of that vertex
	 */
	public JavaRDD<Tuple2<VertexId, Long>> outDegrees() {
		//implemented
		return calDegrees(EdgeDirection.OUT);
	}

	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a
	 *         vertex id with the total degree of that vertex
	 */
	public JavaRDD<Tuple2<VertexId, Long>> degrees() {
		//implemented
		return calDegrees(EdgeDirection.BOTH);
	}
	
	//implemented
	private JavaRDD<Tuple2<VertexId, Long>> calDegrees(EdgeDirection edgeDirection) {
		List<Tuple2<PartitionId, EdgePartition<ED>>> edgepartitions = partitionsRDD.collect();
		List<Tuple2<VertexId, Long>> degrees_result = new ArrayList<Tuple2<VertexId, Long>>();

		Iterator<Tuple2<PartitionId, EdgePartition<ED>>> it_eps = edgepartitions.iterator();
		while (it_eps.hasNext()) {
			Iterator<Tuple2<VertexId, Long>> it_ep = null;
			if (edgeDirection.equals(EdgeDirection.IN)) it_ep = it_eps.next()._2.inDegrees();
			else if (edgeDirection.equals(EdgeDirection.OUT)) it_ep = it_eps.next()._2.outDegrees();
			else if (edgeDirection.equals(EdgeDirection.BOTH)) it_ep = it_eps.next()._2.degrees();		
			while (it_ep.hasNext()) degrees_result.add(it_ep.next());
		}
		
		return SharedJavaSparkContextLocal.jsc().parallelize(degrees_result);
	}

}
