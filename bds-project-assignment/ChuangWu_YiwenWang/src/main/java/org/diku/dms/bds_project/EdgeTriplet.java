package org.diku.dms.bds_project;

import java.io.Serializable;

import scala.Tuple2;
import scala.Tuple3;


/**
 * An edge triplet consists of source vertex id, source vertex attribute, destination vertex id, destination vertex attribute and edge attribute 
 * 
 * @param <ED> edge attribute data type
 * @param <VD> vertex attribute data type
 */
public class EdgeTriplet<ED, VD> extends Edge<ED> implements Serializable {
	public VD srcAttr = null, dstAttr = null;

	/*public EdgeTriplet() {
	}
	*/
	/**
	 * Constructor function.
	 * 
	 * @param srcVertex  the source vertex
	 * @param dstVertex the destination vertex 
	 * @param attr the edge attribute
	 */
	public EdgeTriplet(Tuple2<VertexId, VD> srcVertex, Tuple2<VertexId, VD> dstVertex, ED attr) {
		super(srcVertex._1, dstVertex._1, attr);
		srcAttr = srcVertex._2;
		dstAttr = dstVertex._2;
	}
	
	/**
	 * Given the id of one of two vertices on the edge, return its vertex attribute.
	 * 
	 * @param vid the id of one of two vertices on the edge
	 * @return the attribute associated with the vertex of `vid`  
	 */
	public VD vertexAttr(VertexId vid) {
		if (srcId == vid)
			return srcAttr;
		assert (dstId == vid);
		return dstAttr;
	}
	
	/**
	 * Given the id of one of two vertices on the edge, return attribute of the other vertex.
	 * 
	 * @param vid the id of one of two vertices on the edge
	 * @return the attribute associated to the other vertex on the edge  
	 */
	public VD otherVertexAttr(VertexId vid) {
		if (srcId == vid)
			return dstAttr;
		assert (dstId == vid);
		return srcAttr;
	}
	
	/**
	 * Convert `EdgeTriplet` to a tuple.
	 * @return a tuple that represents `EdgeTriplet`
	 */
	public Tuple3<Tuple2<VertexId, VD>, Tuple2<VertexId, VD>, ED> toTuple() {
		return new Tuple3<Tuple2<VertexId, VD>, Tuple2<VertexId, VD>, ED>(
				new Tuple2<VertexId, VD>(srcId, srcAttr), new Tuple2<VertexId, VD>(dstId, dstAttr), attr);
	}
}
