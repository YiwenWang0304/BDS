package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.query.EdgePattern;
import org.diku.dms.bds_project.query.QueryVertex;
import org.diku.dms.bds_project.query.VertexPredicate;
import org.diku.dms.bds_project.query.VertexPredicate.Type;

import scala.Tuple2;

/**
 * EdgeTripletPartition stores a collections of EdgeTriplet.
 *
 * @param <ED> the edge attribute data type
 * @param <VD> the vertex attribute data type
 */
@SuppressWarnings("serial")
public class EdgeTripletPartition<ED, VD> extends EdgePartition<ED> implements Serializable{
	private VD[] vertexAttrs;
	
	/**
	 * 
	 * @param localSrcIds the local source vertex id of each edge as an index into `local2global` and `vertexAttr`
	 * @param localDstIds the local destination vertex id of each edge as an index into `local2global` and `vertexAttr`
	 * @param data the attribute associated with each edge
	 * @param index the clustered index on source vertex id as a map from each global source vertex id to the offset in the edge arrays where the cluster for that vertex id begins 
	 * @param global2local a mapping from global vertex id to referenced vertex id
	 * @param local2global a mapping from local vertex id to global vertex id
	 * @param vertexAttrs attribute of vertices stored in this `EdgePartition`
	 */
	public EdgeTripletPartition(int[] localSrcIds, int[] localDstIds, ED[] data, Map<VertexId, Integer> index,
			Map<VertexId, Integer> global2local, VertexId[] local2global, VD[] vertexAttrs) {
		super(localSrcIds,localDstIds,data,index,global2local,local2global);
		this.vertexAttrs = vertexAttrs;
	}
	
	/**
	 * Given a edge pattern, return an iterator of vertex id pairs, each having the corresponding edge triplets can match this edge pattern. 
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in `EdgeTripletPartition` try to match
	 * @return the iterator of vertex id pairs, which represents the edge triplet connected by these two vertices can match   
	 */
	@SuppressWarnings("rawtypes")
	public Iterator<Tuple2<VertexId, VertexId>> matchEdgePattern(EdgePattern edgePattern) {
		//implemented
		List<Tuple2<VertexId, VertexId>> matches=new ArrayList<Tuple2<VertexId, VertexId>>(null);
		
		QueryVertex srcVertex=edgePattern.srcVertex;
		QueryVertex dstVertex=edgePattern.dstVertex;
		VertexId srcid=srcVertex.id;
		VertexId dstid=dstVertex.id;
		VertexPredicate srcpredicate=srcVertex.predicate;
		VertexPredicate dstpredicate=dstVertex.predicate;
		
		if(srcpredicate.type.equals(VertexPredicate.Type.ATTR)){//if require src match attr
			for(int i=0;i<this.vertexAttrs.length;i++) {
				if(this.vertexAttrs[i]==srcpredicate.value){//find a vertex's attr matches src's attr
					if(dstpredicate.type.equals(VertexPredicate.Type.ATTR)){//if require dst match attr
						for(int j=0;j<this.vertexAttrs.length;j++) {
							if(dstpredicate.type.equals(VertexPredicate.Type.ATTR)) //find a vertex's attr matches dst's attr
								matches.add(new Tuple2<VertexId, VertexId>(super.local2global[i],super.local2global[j]));	
						}			
					}else if(dstpredicate.type.equals(VertexPredicate.Type.ID)){//if require dst match vid
						for(VertexId id:super.local2global) {
							if(id==dstid)//find a vertex's vid matches dst's vid
								matches.add(new Tuple2<VertexId, VertexId>(super.local2global[i],id));			
						}
					}
				}
			}
		}else if(srcpredicate.type.equals(VertexPredicate.Type.ID)){//if require src match vid
			for(VertexId id:super.local2global) {
				if(id==srcid) {//find a vertex's vid matches src's vid
					if(dstpredicate.type.equals(VertexPredicate.Type.ATTR)){//if require dst match attr
						for(int j=0;j<this.vertexAttrs.length;j++) {
							if(dstpredicate.type.equals(VertexPredicate.Type.ATTR)) //find a vertex's attr matches dst's attr
								matches.add(new Tuple2<VertexId, VertexId>(id,super.local2global[j]));	
						}		
					}else if(dstpredicate.type.equals(VertexPredicate.Type.ID)){//if require dst match vid
						for(VertexId id2:super.local2global) {
							if(id2==dstid) //find a vertex's vid matches dst's vid
								matches.add(new Tuple2<VertexId, VertexId>(id,id2));	
						}
					}
				}
			}
		}
		return matches.iterator();
	}
	
	/**
	 * Given a collection of edges (without vertex attributes) and a collection of vertices (with vertex attributes), merge them into a new `EdgeTripletPartition` which contains both vertex attributes and edge attributes.
	 * 
	 * @param <ED> edge attribute data type
	 * @param <VD> vertex attribute data type
	 * @param edgePartition an edge partition consists of edges without corresponding vertex attributes
	 * @param vertices an iterator of vertices (with vertex attributes) contained in `EdgeParition`
	 * @return a new `EdgeTripletParition`
	 */ 
	@SuppressWarnings("unchecked")
	public static <ED, VD> EdgeTripletPartition<ED, VD> fromEdgePartitionAndVertices(
			EdgePartition<ED> edgePartition,
			scala.collection.Iterator<Tuple2<VertexId, VD>> vertices) {
		//implemented
		VD[] vertexAttrs = (VD[]) new Object[vertices.size()];
		while(vertices.hasNext()) 
			vertexAttrs[edgePartition.global2local.get(vertices.next()._1)]=vertices.next()._2;
		
		EdgeTripletPartition<ED, VD> edgetripletpartition=new EdgeTripletPartition<ED, VD>(
				edgePartition.localSrcIds, edgePartition.localDstIds,edgePartition.data, 
				edgePartition.index,edgePartition.global2local, edgePartition.local2global, vertexAttrs);
		return edgetripletpartition;
	}

	//implemented
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public scala.collection.Iterator iterator() {
		return new scala.collection.AbstractIterator<EdgeTriplet<ED, VD>>() {
			private int pos = 0;

			@Override
			public boolean hasNext() {
				return pos < EdgeTripletPartition.this.size;
			}

			@Override
			public EdgeTriplet<ED, VD> next() {
				EdgeTriplet<ED, VD> edgetriple = new EdgeTriplet<ED, VD>(
						new Tuple2<VertexId, VD>(local2global[localSrcIds[pos]], vertexAttrs[localSrcIds[pos]]), 
						new Tuple2<VertexId, VD>(local2global[localDstIds[pos]], vertexAttrs[localDstIds[pos]]), 
						data[pos]);
				pos++;
				return edgetriple;
			}
		};
	}
	
	public VD getVertexAttrs(int i){
		return  vertexAttrs[i];
	}
}
