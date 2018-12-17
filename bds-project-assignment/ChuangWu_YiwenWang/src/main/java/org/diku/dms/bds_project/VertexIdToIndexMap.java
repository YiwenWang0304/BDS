package org.diku.dms.bds_project;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * A bijection map between vertex ids and the index (offset in some data structure).
 *
 */
@SuppressWarnings("serial")
public class VertexIdToIndexMap extends HashMap<VertexId, Integer> {
	private Map<Integer, VertexId> backwardMap; // used for backward mapping

	public VertexIdToIndexMap() {
		backwardMap = new HashMap<Integer, VertexId>();
	}

	public void put(VertexId vid, int value) throws Exception {
		if (super.containsKey(vid)) throw new Exception("Key " + vid +" already exists in VertexIdToIndexMap");
		if (backwardMap.containsKey(value)) throw new Exception("Value " + value + "already exists in VertexIdtoIndexMap");
		super.put(vid, value); // change both forward and backward mapping
		backwardMap.put(value, vid); 
	}

	public VertexId getValue(int value) {
		return backwardMap.get(value); // get the key for a specific value
	}
}
