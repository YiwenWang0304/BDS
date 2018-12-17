package org.diku.yuan.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.SharedJavaSparkContextLocal;
import org.diku.dms.bds_project.VertexId;
import org.junit.Test;

import scala.Tuple2;


@SuppressWarnings("serial")
public class EdgeRDDTest extends SharedJavaSparkContextLocal implements Serializable {
	
	public List<Edge<Integer>> sampleEdges() {
		return Arrays.asList(
				new Edge<Integer>(new VertexId(2L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(2L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(3L), new VertexId(5L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(3L), 1)
			);
	}
	
	@Test
	public void testEdge() {
		List<Edge<Integer>> testEdges = sampleEdges();
		List<Edge<Integer>> sortedEdges = new ArrayList<Edge<Integer>>();
		for (Edge<Integer> edge : testEdges) {
			sortedEdges.add(edge);
		}
		Collections.sort(sortedEdges, Edge.comparator);
		assert(sortedEdges.get(0).compareTo(testEdges.get(1)) == 0);
		assert(sortedEdges.get(1).compareTo(testEdges.get(4)) == 0);
		assert(sortedEdges.get(2).compareTo(testEdges.get(2)) == 0);
		assert(sortedEdges.get(3).compareTo(testEdges.get(0)) == 0);
		assert(sortedEdges.get(4).compareTo(testEdges.get(3)) == 0);
	}
	
	@Test
	public void testEdgeRDD() {
		List<Edge<Integer>> testEdges = sampleEdges();
		JavaRDD<Edge<Integer>> edges = jsc().parallelize(testEdges, 2);
		EdgeRDD<Integer> edgeRDD = EdgeRDD.fromEdges(edges);
		@SuppressWarnings("unchecked")
		List<Edge<Integer>> collectedEdges = Arrays.asList((Edge<Integer>[]) edgeRDD.collect());
		Collections.sort(collectedEdges, Edge.comparator);
		assert(collectedEdges.get(0).compareTo(testEdges.get(1)) == 0);
		assert(collectedEdges.get(1).compareTo(testEdges.get(4)) == 0);
		assert(collectedEdges.get(2).compareTo(testEdges.get(2)) == 0);
		assert(collectedEdges.get(3).compareTo(testEdges.get(0)) == 0);
		assert(collectedEdges.get(4).compareTo(testEdges.get(3)) == 0);
	}
	
	//implemented
	@Test
	public void testDegree() {
		List<Edge<Integer>> testEdges = sampleEdges();
		JavaRDD<Edge<Integer>> edges = jsc().parallelize(testEdges);
		EdgeRDD<Integer> edgeRDD = EdgeRDD.fromEdges(edges);
		List<Tuple2<VertexId, Long>> in=edgeRDD.inDegrees().collect();
		List<Tuple2<VertexId, Long>> out=edgeRDD.outDegrees().collect();
		List<Tuple2<VertexId, Long>> both=edgeRDD.degrees().collect();
		Iterator<Tuple2<VertexId, Long>> inItr=in.iterator();
		Iterator<Tuple2<VertexId, Long>> outItr=out.iterator();
		Iterator<Tuple2<VertexId, Long>> bothItr=both.iterator();
		while(inItr.hasNext()) {//test indegree
			VertexId vid=inItr.next()._1;
			if(vid.equals(1)) 
				assert(inItr.next()._2==3);
			else if(vid.equals(2)) 
				assert(inItr.next()._2==1);
			else if(vid.equals(3)) 
				assert(inItr.next()._2==1);
		}
		while(outItr.hasNext()) {//test outdegree
			VertexId vid=outItr.next()._1;
			if(vid.equals(2)) 
				assert(outItr.next()._2==1);
			else if(vid.equals(3)) 
				assert(outItr.next()._2==1);
			else if(vid.equals(4)) 
				assert(outItr.next()._2==2);
			else if(vid.equals(5)) 
				assert(outItr.next()._2==1);
		}
		while(bothItr.hasNext()) {//test degree
			VertexId vid=bothItr.next()._1;
			if(vid.equals(1)) 
				assert(bothItr.next()._2==3);
			else if(vid.equals(2)) 
				assert(bothItr.next()._2==2);
			else if(vid.equals(3)) 
				assert(bothItr.next()._2==2);
			else if(vid.equals(4)) 
				assert(bothItr.next()._2==2);
			else if(vid.equals(5)) 
				assert(bothItr.next()._2==1);
		}	
	}
	
	
}
