package org.diku.dms.bds_project.query;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.diku.dms.bds_project.VertexId;

/**
 * `Match` is the data type stored in MatchesRDD to represent a match of an edge pattern, a partial pattern or even the entire pattern graph.
 */
@SuppressWarnings("serial")
public class Match implements Serializable {
	//implemented
	public List<VertexId> vertexs = null;
	
	public Match(List<VertexId> vertexs) {
		this.vertexs = vertexs;
	}

	public int compareTo(VertexId othervid) {
		for(int index=0;index<vertexs.size();index++) {
		 if (vertexs.get(index).compareTo(othervid) > 0) 
			return 1;
		 else if (vertexs.get(index).compareTo(othervid)< 0)
			return -1;
		} 
		return 0;
	}
	
	public Iterator<VertexId> iterator(){
		return vertexs.iterator();
	}
	 
}