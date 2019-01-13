package org.diku.dms.bds_project;

import java.io.Serializable;

/**
 * PartitionId encapsulates the id of a partition in an RDD.
 */
public class PartitionId implements Serializable {
	public Integer id = 0;

	public PartitionId(int id) {
		this.id = id;
	}

	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if (!(other instanceof PartitionId)) {
			return false;
		}
		long otherId = ((PartitionId) other).id;
		return otherId == id;
	}

	public int hashCode() {
		return id.hashCode();
	}
}
