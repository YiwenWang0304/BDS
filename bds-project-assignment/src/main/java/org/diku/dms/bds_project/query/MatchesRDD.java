package org.diku.dms.bds_project.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.*;

import scala.Tuple2;

/**
 * A MatchesRDD instance stores a collection of matches to a partial pattern as
 * an RDD.
 * 
 * @param <VD>
 * @param <ED>
 */
@SuppressWarnings("serial")
public class MatchesRDD<VD, ED> extends JavaRDD<Match> {

	public MatchMeta meta; // DONE
	public List<Match> matches; // DONE

	/**
	 * Constructor method.
	 * 
	 * @param matches a JavaRDD consists of many `Match` instances.
	 */
	public MatchesRDD(MatchMeta meta, JavaRDD<Match> matches) {
		// implemented
		super(matches.rdd(), scala.reflect.ClassTag$.MODULE$.apply(Match.class));
		List<Match> listMatches = matches.collect();
		Iterator<Match> it = listMatches.iterator();
		this.meta = meta;
		while (it.hasNext())
			this.matches.add(it.next());
	}

	/**
	 * The method `join()` computes the joining results of two MatchesRDD instances,
	 * result is returned as a new MatchesRDD instance. For instance, in the example
	 * of assignment description, the matches of edge pattern (u1, u2) and (u1, u3)
	 * are as follows.
	 * 
	 * |u1 u2 | |u1 u3| ----- meta data ---| |------| |-----| ---| |v2 v3 | |v2 v4|
	 * --| ---|-- MatchesRDD instance |v3 v4 | |v4 v5| --|-- collection of `Match`
	 * ---| |v5 v3 | | | --| ---|
	 * 
	 * The result of join should be |u1 u2 u3| |--------| |v2 v3 v4| | |
	 * 
	 * @param other another `MatchesRDD` instance
	 * @return a new `MatchesRDD` according to the join
	 */
	@SuppressWarnings("null")
	public MatchesRDD<ED, VD> join(MatchesRDD<ED, VD> other) {
		// Implemented.
		MatchesRDD<ED, VD> newmatchRDD = new MatchesRDD<ED, VD>(null, null);
		int[] pos = this.meta.compareWith(other.meta);
		if (pos[0] != -1) {//at lease one same vertex in both MatchesRDD
			// generate new matchRDD's meta
			newmatchRDD.meta = this.meta;
			Iterator<VertexId> it0 = newmatchRDD.meta.iterator();
			while (it0.hasNext()) {
				VertexId thisVertexId = it0.next();
				Iterator<VertexId> it1 = other.meta.iterator();
				while (it1.hasNext()) {
					VertexId otherVertexId = it1.next();
					if (thisVertexId == otherVertexId)
						break;
					else
						newmatchRDD.meta.vertexs.add(otherVertexId);
				}
			}

			Iterator<Match> itmatchother = other.matches.iterator();
			Iterator<Match> itmatch = this.matches.iterator();
			while (itmatch.hasNext()) {
				Match thismatch = itmatch.next();
				while (itmatchother.hasNext()) {
					Match othermatch = itmatchother.next();
					if (thismatch.vertexs.get(pos[0]).compareTo(othermatch.vertexs.get(pos[1])) == 0) {//compare matches to same meta vertex
						// generate new matchRDD's matches
						Match newmatch = thismatch;
						Iterator<VertexId> it2 = thismatch.vertexs.iterator();
						Iterator<VertexId> it3 = othermatch.vertexs.iterator();
						while (it2.hasNext()) {
							VertexId thisVertexId = it2.next();
							while (it3.hasNext()) {
								VertexId otherVertexId = it3.next();
								if (thisVertexId == otherVertexId)
									break;
								else
									newmatch.vertexs.add(otherVertexId);
							}
						}
						newmatchRDD.matches.add(newmatch);
					}
				}
			}
		}
		return newmatchRDD;
	}

	// implemented
	@SuppressWarnings({ "rawtypes" })
	public static MatchesRDD matchEdgePattern() throws NumberFormatException, Exception {
		GraphLoader graphLoader = new GraphLoader();// create a graphloader instance
		Graph graph = graphLoader.getGraphInstance("/home/ywang/");// get a graph instance
		PatternGraph patterngraph = graphLoader.getPatternInstance("file_dir");// get pattern instance

		return graph.match(patterngraph);
	}

	/*
	 * // implemented public JavaRDD<Tuple2<VertexId, VertexId>>
	 * getAllEdgesByLabels(VD vertexLabel, ED edgeLabel, EdgeDirection
	 * edgeDirection) { List<Tuple2<VertexId, VertexId>> alledgesbylabels = new
	 * ArrayList<Tuple2<VertexId, VertexId>>();
	 * 
	 * List<Tuple2<PartitionId, EdgeTripletPartition<ED, VD>>> edgeTripletPartitions
	 * = edgeTriplets.partitionsRDD .collect(); Iterator<Tuple2<PartitionId,
	 * EdgeTripletPartition<ED, VD>>> edgetripletItr =
	 * edgeTripletPartitions.iterator();
	 * 
	 * while (edgetripletItr.hasNext()) { EdgeTripletPartition<ED, VD>
	 * edgetripletpartition = edgetripletItr.next()._2; List<Integer> edgepos = new
	 * ArrayList<Integer>(); int i = 0; for (ED edgelabel :
	 * edgetripletpartition.data) { if (edgelabel == edgeLabel) edgepos.add(i); i++;
	 * }
	 * 
	 * Iterator<Integer> posItr = edgepos.iterator(); while (posItr.hasNext()) {
	 * Integer pos = posItr.next(); VertexId src =
	 * edgetripletpartition.local2global[edgetripletpartition.localSrcIds[pos]];
	 * VertexId dst =
	 * edgetripletpartition.local2global[edgetripletpartition.localDstIds[pos]]; if
	 * (edgeDirection.equals(EdgeDirection.IN)) {// check source vertex if
	 * (vertexLabel ==
	 * edgetripletpartition.getVertexAttrs(edgetripletpartition.localSrcIds[pos]))
	 * alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst)); } else if
	 * (edgeDirection.equals(EdgeDirection.OUT)) {// check destination vertex if
	 * (vertexLabel ==
	 * edgetripletpartition.getVertexAttrs(edgetripletpartition.localDstIds[pos]))
	 * alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst)); } else if
	 * (edgeDirection.equals(EdgeDirection.BOTH)) {// check both vertices if
	 * (vertexLabel ==
	 * edgetripletpartition.getVertexAttrs(edgetripletpartition.localSrcIds[pos]) &&
	 * vertexLabel == edgetripletpartition
	 * .getVertexAttrs(edgetripletpartition.localDstIds[pos]))
	 * alledgesbylabels.add(new Tuple2<VertexId, VertexId>(src, dst)); } } } return
	 * SharedJavaSparkContextLocal.jsc().parallelize(alledgesbylabels);
	 * 
	 * }
	 * 
	 * // implemented public JavaRDD<Tuple2<VertexId, VertexId>>
	 * getEdgesFromVertexByLabels(VertexId vertexId, ED edgeLabel, EdgeDirection
	 * edgeDirection) { List<Tuple2<VertexId, VertexId>> edgesfromvertexbylabels =
	 * new ArrayList<Tuple2<VertexId, VertexId>>();
	 * 
	 * List<Tuple2<PartitionId, EdgeTripletPartition<ED, VD>>> edgeTripletPartitions
	 * = edgeTriplets.partitionsRDD .collect(); Iterator<Tuple2<PartitionId,
	 * EdgeTripletPartition<ED, VD>>> edgetripletItr =
	 * edgeTripletPartitions.iterator();
	 * 
	 * while (edgetripletItr.hasNext()) { EdgeTripletPartition<ED, VD>
	 * edgetripletpartition = edgetripletItr.next()._2; List<Integer> edgepos = new
	 * ArrayList<Integer>(); int i = 0; for (ED edgelabel :
	 * edgetripletpartition.data) { if (edgelabel == edgeLabel) edgepos.add(i); i++;
	 * }
	 * 
	 * Iterator<Integer> posItr = edgepos.iterator(); while (posItr.hasNext()) {
	 * VertexId src =
	 * edgetripletpartition.local2global[edgetripletpartition.localSrcIds[posItr.
	 * next()]]; VertexId dst =
	 * edgetripletpartition.local2global[edgetripletpartition.localDstIds[posItr.
	 * next()]]; if (edgeDirection.equals(EdgeDirection.IN)) {// check source vertex
	 * if (vertexId == src) edgesfromvertexbylabels.add(new Tuple2<VertexId,
	 * VertexId>(src, dst)); } else if (edgeDirection.equals(EdgeDirection.OUT)) {//
	 * check destination vertex if (vertexId == dst) edgesfromvertexbylabels.add(new
	 * Tuple2<VertexId, VertexId>(src, dst)); } else {// check both vertices if
	 * (vertexId == src && vertexId == dst) edgesfromvertexbylabels.add(new
	 * Tuple2<VertexId, VertexId>(src, dst)); } } }
	 * 
	 * return
	 * SharedJavaSparkContextLocal.jsc().parallelize(edgesfromvertexbylabels); }
	 */
}