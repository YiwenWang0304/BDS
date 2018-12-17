package org.diku.dms.bds_project.query;

import org.diku.dms.bds_project.VertexId;

/**
 * A query edge consists of ids of two query vertices and the attribute predicate on the query edge.
 * The attribute predicate means the attribute on the data edge must equal `attr`.
 * 
 * @param <ED> edge attribute data type
 */
public class QueryEdge<ED> {
	public VertexId srcId, dstId;
	public ED attr;
	/**
	 * 
	 * @param srcId id of the source query vertex on the query edge
	 * @param dstId id of the destination query vertex on the query edge
	 * @param attr the attribute predicate on the query edge
	 */
	public QueryEdge(VertexId srcId, VertexId dstId, ED attr) {
		this.srcId = srcId;
		this.dstId = dstId;
		this.attr = attr;
	}
}
