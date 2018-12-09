package org.diku.dms.bds_project.query;

/**
 * `type` refers to whether this query vertex should match a data vertex id or a vertex attribute.
 * `value` represents either a vertex id or a vertex attribute according to `type`.
 */
public class VertexPredicate<V> {
	public static enum Type {ATTR, ID};
	public Type type; 
	public Object value;
	/**
	 * 
	 * @param type the predicate type of the query vertex
	 * @param value the value of the query vertex predicate
	 */
	public VertexPredicate(Type type, Object value) {
		this.type = type;
		this.value = value;
	}
}
