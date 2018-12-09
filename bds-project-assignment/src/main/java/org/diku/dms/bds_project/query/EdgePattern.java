package org.diku.dms.bds_project.query;

import java.io.Serializable;

/**
 * An edge pattern consists of source vertex, destination vertex and edge attribute.
 * `srcVertex` and `dstVertex` are typed as `QueryVertex`, which should either match the vertex id or the vertex attribute.
 * `attr` represents the predicate on the edge, representing the attribute associated with the data edge should match `attr`.  
 * 
 * @param <VD> vertex attribute data type
 * @param <ED> edge attribute data type
 */
@SuppressWarnings("serial")
public class EdgePattern<VD, ED> implements Serializable{
	@SuppressWarnings("rawtypes")
	public QueryVertex srcVertex, dstVertex;
	public ED attr;
	/**
	 * 
	 * @param srcVertex source vertex
	 * @param dstVertex destination vertex
	 * @param attr edge attribute
	 */
	@SuppressWarnings("rawtypes")
	public EdgePattern(QueryVertex srcVertex, QueryVertex dstVertex, ED attr) {
		this.srcVertex = srcVertex;
		this.dstVertex = dstVertex;
		this.attr = attr;
	}
	
}
