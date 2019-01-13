package org.diku.dms.bds_project.query;

import org.diku.dms.bds_project.VertexId;

/**
 * A query vertex consists of a vertex id and a predicate on vertex for match. 
 * 
 */
public class QueryVertex<V> {
	public VertexId id;
	public VertexPredicate<V> predicate;
	/**
	 * 
	 * @param id query vertex id 
	 * @param predicate predicate on the query vertex for match
	 */
	public QueryVertex(VertexId id, VertexPredicate<V> predicate) {
		this.id = id;
		this.predicate = predicate;
	}
}
