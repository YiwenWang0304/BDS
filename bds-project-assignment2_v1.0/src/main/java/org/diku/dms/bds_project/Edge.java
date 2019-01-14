package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.Comparator;

/**
 * An edge consists of the id of source vertex, the id of destination vertex and the attribute on the edge.
 *
 * @param <ED> edge attribute data type
 */
public class Edge<ED> implements Comparable<Edge<ED>>, Serializable {
	
	public static Comparator<Edge> comparator = new Comparator<Edge>() {
		public int compare(Edge o1, Edge o2) {
			return o1.compareTo(o2);
		}
	};
	
	public VertexId srcId = null, dstId = null;
	public ED attr = null;
	
	/**
	 * Constructor method of Edge. 
	 * 
	 * @param srcId the vertex id of the source vertex
	 * @param dstId the vertex id of the destination vertex
	 * @param attr the attribute associated with the edge
	 */
	public Edge(VertexId srcId, VertexId dstId, ED attr) {
		this.srcId = srcId;
		this.dstId = dstId;
		this.attr = attr;
	}
	
	/**
	 * Given one vertex in the edge return the other vertex.
	 * 
	 * @param vid the id one of two vertices on the edge.
	 * @return the id of the other vertex on the edge.
	 */
	public VertexId otherVertexId(VertexId vid) {
		if (srcId == vid)
			return dstId;
		else {
			assert (dstId == vid);
			return srcId;
		}
	}

	/**
	 * Return the relative direction of the edge to the corresponding vertex.
	 *  
	 * @param vid the id of one of the two vertices in the edge.
	 * @return the relative direction of the edge to the corresponding vertex.
	 */
	public EdgeDirection relativeDirection(VertexId vid) {
		if (vid == srcId)
			return EdgeDirection.OUT;
		assert (vid == dstId);
		return EdgeDirection.IN;
	}
	

	public int compareTo(Edge<ED> other) {
		if (srcId.compareTo(other.srcId) == 0) {
			if (dstId.compareTo(other.dstId) == 0)
				return 0;
			else if (dstId.compareTo(other.dstId) < 0)
				return -1;
			else
				return 1;
		} else if (srcId.compareTo(other.srcId) < 0)
			return -1;
		return 1;
	}
}
