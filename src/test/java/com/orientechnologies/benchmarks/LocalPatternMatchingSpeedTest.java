/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */
package com.orientechnologies.benchmarks;

import com.orientechnologies.benchmarks.base.AbstractGraphBenchmark;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

public class LocalPatternMatchingSpeedTest extends AbstractGraphBenchmark {
  public LocalPatternMatchingSpeedTest() {
    super("PatternMatching", 50000);
  }

  public void test() {
    String versionString = OConstants.getVersion();
    String[] splitted = versionString.split("\\.");
    if (splitted.length < 3) {
      return;
    }
    try {
      int major = Integer.parseInt(splitted[0]);
      int minor = Integer.parseInt(splitted[1]);
      if (major < 2) {
        return;
      }
      if (major == 2 && minor < 2) {
        return;
      }

    } catch (Exception e) {
      return;
    }
    final OrientBaseGraph graph = createGraphNoTx();

    initSchema(graph);

    step("MatchOutOnSupernodesEmpty", new Step() {
      @Override
      public void execute(final long items) {
        matchOutOnSupernodes(graph, items);
      }
    });

    step("MatchOutOnSupernodesCreateDataset", new Step() {
      @Override
      public void execute(final long items) {
        initTestNoIndex(graph, items);
      }
    });

    step("MatchOutOnSupernodesNoIndex", new Step() {
      @Override
      public void execute(final long items) {
        matchOutOnSupernodes(graph, items);
      }
    });

    initIndexes(graph);
    step("MatchOutOnSupernodesWithIndex", new Step() {
      @Override
      public void execute(final long items) {
        matchOutOnSupernodes(graph, items);
      }
    });
    end();
  }

  private void initIndexes(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE index IndexedEdge_out_in on IndexedEdge (out, in) NOTUNIQUE")).execute();
  }

  private void initSchema(OrientBaseGraph db) {

    db.command(new OCommandSQL("CREATE class IndexedVertex extends V")).execute();
    db.command(new OCommandSQL("CREATE property IndexedVertex.uid INTEGER")).execute();
    db.command(new OCommandSQL("CREATE index IndexedVertex_uid on IndexedVertex (uid) NOTUNIQUE")).execute();

    db.command(new OCommandSQL("CREATE class IndexedEdge extends E")).execute();
    db.command(new OCommandSQL("CREATE property IndexedEdge.out LINK")).execute();
    db.command(new OCommandSQL("CREATE property IndexedEdge.in LINK")).execute();
  }

  private void initTestNoIndex(OrientBaseGraph db, long items) {
    Vertex doc0 = db.addVertex("class:IndexedVertex");
    doc0.setProperty("uid", 0);
    Vertex doc1 = db.addVertex("class:IndexedVertex");
    doc1.setProperty("uid", 1);
    for (int i = 2; i < items; i++) {
      Vertex doc = db.addVertex("class:V");
      doc.setProperty("uid", i);
      doc0.addEdge("IndexedEdge", doc);
      doc.addEdge("IndexedEdge", doc1);
      if (i % 1000 == 0) {
        db.commit();
      }
    }
    db.commit();

  }

  protected void matchOutOnSupernodes(final OrientBaseGraph graph, final long items) {
    graph.command(new OSQLSynchQuery<Vertex>(""));
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)}");
    query.append(".out('IndexedEdge'){class:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");
    Iterable<?> result = graph.command(new OCommandSQL(query.toString())).execute();
  }

}
