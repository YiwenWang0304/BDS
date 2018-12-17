package org.diku.dms.bds_project.query;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.Graph;
import org.diku.dms.bds_project.SharedJavaSparkContextLocal;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.VertexRDD;

import scala.Tuple2;

/**
 * A loader than can read data from WHERE?
 * 
 *
 */
//implemented
public class GraphLoader<VD, ED> {
	
	GraphLoader() {}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Graph<VD, ED> getGraphInstance(String file_dir) throws NumberFormatException, Exception {
		File file = new File(file_dir);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		reader.readLine(); // this will read the first line
		String line=null;
		
		List<Edge<ED>> edgeslist=new ArrayList<Edge<ED>>(null);
		List<Tuple2<VertexId,VD>> vertexslist=new ArrayList<Tuple2<VertexId,VD>>(null);
		while ((line=reader.readLine())!= null) { // loop will run from 2nd line
			String[] tmp = line.split(" ");
			if (tmp[0] == "v") {// vertex
				Tuple2<VertexId,VD> vertex= new Tuple2<VertexId,VD>(new VertexId(Integer.parseInt(tmp[1])),(VD) tmp[2]);
				vertexslist.add(vertex);
			} else {// edge
				VertexId src=new VertexId(Integer.parseInt(tmp[1]));
				VertexId dst=new VertexId(Integer.parseInt(tmp[2]));
				Edge e=new Edge(src,dst,(ED)tmp[3]);
				edgeslist.add(e);
			}	
		}
		
		JavaRDD<Edge<ED>> edges=SharedJavaSparkContextLocal.jsc().parallelize(edgeslist);
		JavaPairRDD<VertexId, VD> vertices=SharedJavaSparkContextLocal.jsc().parallelizePairs(vertexslist);
		
		return Graph.fromEdgesAndVetices(edges, vertices);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public PatternGraph<VD, ED> getPatternInstance(String file_dir) throws IOException {
		File file = new File(file_dir);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		reader.readLine(); // this will read the first line
		String line = null;
		
		List<QueryVertex> vertices=new ArrayList<QueryVertex>(null);
		List<QueryEdge> edges=new ArrayList<QueryEdge>(null);
		while ((line=reader.readLine())!= null) { // loop will run from 2nd line
			String[] tmp = line.split(" ");
			if (tmp[0] == "v") {// vertex
				VertexId vid=new VertexId(Integer.parseInt(tmp[1]));
				VertexPredicate.Type type=null;
				Object value=null;
				if(tmp[3].equals("-1")) {//match attr
					type=VertexPredicate.Type.ATTR;
					value=tmp[2];
				}else {//match vid
					type=VertexPredicate.Type.ID;
					value=tmp[1];
				}
				VertexPredicate vp=new VertexPredicate(type, value);
				QueryVertex vertex= new QueryVertex(vid, vp) ;
				vertices.add(vertex);
			} else {// edge
				VertexId src=new VertexId(Integer.parseInt(tmp[1]));
				VertexId dst=new VertexId(Integer.parseInt(tmp[2]));
				QueryEdge<ED> edge=new QueryEdge(src,dst,(ED)tmp[3]);
				edges.add(edge);
			}	
		}
			
		PatternGraph<VD, ED> patterngraph=new PatternGraph<VD, ED>(vertices, edges);
		
		return patterngraph;
	}
}
