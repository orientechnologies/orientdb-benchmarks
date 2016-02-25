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

import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import com.orientechnologies.benchmarks.base.AbstractGraphBenchmark;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Test;

public class LocalTraversalSpeedTest extends AbstractGraphBenchmark {
  private OrientVertex root;
  private AtomicLong   counter = new AtomicLong();

  public LocalTraversalSpeedTest() {
    super("Vertices");
  }

  @Test
  public void test() {
    final OrientBaseGraph graph = createGraphNoTx();

    step("populate", new Step() {
      @Override
      public void execute(final long items) {
        populate(graph, items);
      }
    });

    step("traverseSpeed", new Step() {
      @Override
      public void execute(final long items) {
        traverseSpeed(graph, items);
      }
    });

    end();
  }

  protected void traverseSpeed(final OrientBaseGraph graph, final long items) {
    Iterable<OrientVertex> result = (Iterable<OrientVertex>) graph
        .command(new OCommandSQL("select count(*) from ( traverse out() from " + root.getIdentity() + ")"));
    final Number count = result.iterator().next().getProperty("count");

    Assert.assertEquals(count.longValue(), counter.longValue());
  }

  protected static void addNodes(final OrientBaseGraph graph, final OrientVertex v, final int children, final int currLevel,
      final int maxLevel, final AtomicLong counter, final long maxItems) {
    OLogManager.instance().info(null, "Populating tree level %d of %d counter=%d maxItems=%d", currLevel, maxLevel, counter.get(),
        maxItems);

    for (int i = 0; i < children; ++i) {
      final OrientVertex child = graph.addVertex("class:Node", "level", currLevel, "childNum", i);
      counter.incrementAndGet();

      v.addEdge("Child", child);

      if (currLevel < maxLevel)
        addNodes(graph, child, children, currLevel + 1, maxLevel, counter, maxItems);
    }
    graph.commit();
  }

  protected void populate(final OrientBaseGraph graph, final long maxItems) {
    graph.setKeepInMemoryReferences(true);
    graph.declareIntent(new OIntentMassiveInsert());
    final OClass cls = graph.createVertexType("Node");
    OClassImpl.addClusters(cls, 8);

    graph.createEdgeType("Child");

    root = graph.addVertex("class:Node", "level", 0, "childNum", 0);
    addNodes(graph, root, 20, 0, 8, counter, maxItems);
    graph.declareIntent(null);
  }
}
