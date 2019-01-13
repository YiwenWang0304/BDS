package org.diku.dms.bds_project;

import java.io.Serializable;

/**
 * VertexId encapsulates the id of a vertex which is a long.
 */
public class VertexId implements Comparable<VertexId>, Serializable {
	public Long id = 0L;

	public VertexId(long id) {
		this.id = id;
	}

	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if (!(other instanceof VertexId)) {
			return false;
		}
		long otherId = ((VertexId) other).id;
		return otherId == id;
	}

	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public int compareTo(VertexId other) {
		return id.compareTo(other.id);
	}

	@Override
	public String toString() {
		return String.valueOf(id);
	}
}
