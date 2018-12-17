package org.diku.dms.bds_project.query;

import java.util.Iterator;
import java.util.List;

import org.diku.dms.bds_project.VertexId;

/**
 * `MatchMeta` contains some necessary information regarding the partial pattern that the data of MatchesRDD matches to.
 * It is used to perform joining over two `MatchesRDD`ss instances, e.g., when you need to know common columns of two partial patterns.
 */
public class MatchMeta {
	//implemented
	public List<VertexId> vertexs = null;
	
	public MatchMeta(List<VertexId> vertexs) {
		this.vertexs = vertexs;
	}
	
	public int[] compareWith(MatchMeta othermeta) {
		for(int i=0;i<this.vertexs.size();i++) {
			for(int j=0;j<othermeta.vertexs.size();i++) {
				if (this.vertexs.get(i).compareTo(othermeta.vertexs.get(j))==0) 
					return new int[] {i,j};
			}
		} 
		return new int[]{-1,-1};
	}
	
	public Iterator<VertexId> iterator(){
		return vertexs.iterator();
	}
	 
}