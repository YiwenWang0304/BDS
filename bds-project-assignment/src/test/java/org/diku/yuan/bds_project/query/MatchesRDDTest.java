package org.diku.yuan.bds_project.query;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.diku.dms.bds_project.SharedJavaSparkContextLocal;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.query.Match;
import org.diku.dms.bds_project.query.MatchMeta;
import org.diku.dms.bds_project.query.MatchesRDD;
import org.junit.Test;

public class MatchesRDDTest {

	public List<List<VertexId>> sampleMeta() {
		List<VertexId> vidlist1=new ArrayList<VertexId>();
		vidlist1.add(new VertexId(1L));
		vidlist1.add(new VertexId(2L));	

		List<VertexId> vidlist2=new ArrayList<VertexId>();
		vidlist2.add(new VertexId(1L));
		vidlist2.add(new VertexId(3L));	
		
		return Arrays.asList(vidlist1,vidlist2);
	}
	
	public List<List<Match>> sampleMatches() {
		List<Match> matches1=new ArrayList<Match>();
		List<VertexId> match1=new ArrayList<VertexId>();
		match1.add(new VertexId(2L));
		match1.add(new VertexId(3L));
		List<VertexId> match2=new ArrayList<VertexId>();
		match2.add(new VertexId(3L));
		match2.add(new VertexId(4L));
		List<VertexId> match3=new ArrayList<VertexId>();
		match3.add(new VertexId(5L));
		match3.add(new VertexId(3L));
		matches1.add(new Match(match1));
		matches1.add(new Match(match2));
		matches1.add(new Match(match3));
		
		List<Match> matches2=new ArrayList<Match>();
		List<VertexId> match4=new ArrayList<VertexId>();
		match4.add(new VertexId(2L));
		match4.add(new VertexId(4L));
		List<VertexId> match5=new ArrayList<VertexId>();
		match5.add(new VertexId(4L));
		match5.add(new VertexId(5L));
		matches2.add(new Match(match4));
		matches2.add(new Match(match5));
		
		return Arrays.asList(matches1,matches2);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMatchesRDD() {
		List<List<VertexId>> vertexidlist=sampleMeta();
		List<List<Match>> matchlist=sampleMatches();
		//|u1 u2 |
		//|------| 
		//|v2 v3 | 
		//|v3 v4 | 
		//|v5 v3 | 
		MatchesRDD matchesRDD1=new MatchesRDD(new MatchMeta(vertexidlist.get(0)),SharedJavaSparkContextLocal.jsc().parallelize(matchlist.get(0)));
		assertTrue(matchesRDD1.meta.vertexs.get(0).compareTo(new VertexId(1L))==0);
		assertTrue(matchesRDD1.meta.vertexs.get(1).compareTo(new VertexId(2L))==0);
		List<Match> matches=matchesRDD1.matches;
		assertTrue(matches.get(0).vertexs.get(0).compareTo(new VertexId(2L))==0);
		assertTrue(matches.get(0).vertexs.get(1).compareTo(new VertexId(3L))==0);
		assertTrue(matches.get(1).vertexs.get(0).compareTo(new VertexId(3L))==0);
		assertTrue(matches.get(1).vertexs.get(1).compareTo(new VertexId(4L))==0);
		assertTrue(matches.get(2).vertexs.get(0).compareTo(new VertexId(5L))==0);
		assertTrue(matches.get(2).vertexs.get(1).compareTo(new VertexId(3L))==0);	
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoin() {
		List<List<VertexId>> vertexidlist=sampleMeta();
		List<List<Match>> matchlist=sampleMatches();
		MatchesRDD matchesRDD1=new MatchesRDD(new MatchMeta(vertexidlist.get(0)),SharedJavaSparkContextLocal.jsc().parallelize(matchlist.get(0)));
		MatchesRDD matchesRDD2=new MatchesRDD(new MatchMeta(vertexidlist.get(1)), SharedJavaSparkContextLocal.jsc().parallelize(matchlist.get(1)));

		MatchesRDD matchesRDD3=matchesRDD1.join(matchesRDD2);//join two matchesRDD
		assertTrue(matchesRDD3.meta.vertexs.get(0).compareTo(new VertexId(1L))==0);//test matchmeta=|u1 u3 u2|
		assertTrue(matchesRDD3.meta.vertexs.get(1).compareTo(new VertexId(3L))==0);
		assertTrue(matchesRDD3.meta.vertexs.get(2).compareTo(new VertexId(2L))==0);
		
		Match match=(Match) matchesRDD3.matches.get(0);
		assertTrue(match.vertexs.get(0).compareTo(new VertexId(2L))==0);//test match=|v2 v4 v3|
		assertTrue(match.vertexs.get(1).compareTo(new VertexId(4L))==0);
		assertTrue(match.vertexs.get(2).compareTo(new VertexId(3L))==0);
	}
	
}
