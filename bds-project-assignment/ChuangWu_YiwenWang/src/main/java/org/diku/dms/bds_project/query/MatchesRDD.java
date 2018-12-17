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
		this.meta = meta;
		this.matches=matches.collect();
	}

	/**
	 * The method `join()` computes the joining results of two MatchesRDD instances,
	 * result is returned as a new MatchesRDD instance. For instance, in the example
	 * of assignment description, the matches of edge pattern (u1, u2) and (u1, u3)
	 * are as follows.
	 * 
	 * |u1 u2 | |u1 u3| ----- meta data ---| 
	 * |------| |-----| ---| 
	 * |v2 v3 | |v2 v4| --| ---|-- MatchesRDD instance 
	 * |v3 v4 | |v4 v5| --|-- collection of `Match` ---| 
	 * |v5 v3 | | | --| ---|
	 * 
	 * The result of join should be 
	 * |u1 u2 u3|
	 * |--------| 
	 * |v2 v3 v4| | |
	 * 
	 * @param other another `MatchesRDD` instance
	 * @return a new `MatchesRDD` according to the join
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MatchesRDD<ED, VD> join(MatchesRDD<ED, VD> other) {
		// Implemented.
		int[] pos = this.meta.compareWith(other.meta);
		MatchesRDD matchesRDD = null;
		
		if (pos[0] != -1) {//at lease one same vertex in both MatchesRDD
			
			// generate new matchRDD's meta		
			List<VertexId> vertexs= new ArrayList<VertexId>();
			for (VertexId vid:this.meta.vertexs) {
				if(!vertexs.contains(vid)) 
					vertexs.add(vid);
				for (VertexId ovid:other.meta.vertexs) {
					if(!vertexs.contains(ovid))  
						vertexs.add(ovid);
				}
			}
			MatchMeta meta=new MatchMeta(vertexs);

			List<Match> matches=new ArrayList<Match>();
			for (Match thismatch:this.matches) {
				for (Match othermatch: other.matches) {
					if (thismatch.vertexs.get(pos[0]).compareTo(othermatch.vertexs.get(pos[1])) == 0) {//compare matches to same meta vertex
						// generate new matchRDD's matches
						List<VertexId> vertexsmatch=new ArrayList<VertexId>();
						for (VertexId thisVertexId:thismatch.vertexs) {
							if(!vertexsmatch.contains(thisVertexId)) 
								vertexsmatch.add(thisVertexId);
							for(VertexId otherVertexId: othermatch.vertexs) {
								if (!vertexsmatch.contains(otherVertexId)) 
									vertexsmatch.add(otherVertexId);
							}
						}
						matches.add(new Match(vertexsmatch));
					}
				}
			}
			matchesRDD=new MatchesRDD(meta,SharedJavaSparkContextLocal.jsc().parallelize(matches));
		}
		return matchesRDD;
	}

	// implemented
	@SuppressWarnings({ "rawtypes" })
	public static MatchesRDD matchEdgePattern() throws NumberFormatException, Exception {
		GraphLoader graphLoader = new GraphLoader();// create a graphloader instance
		Graph graph = graphLoader.getGraphInstance("/home/ywang/");// get a graph instance
		PatternGraph patterngraph = graphLoader.getPatternInstance("file_dir");// get pattern instance

		return graph.match(patterngraph);
	}

}