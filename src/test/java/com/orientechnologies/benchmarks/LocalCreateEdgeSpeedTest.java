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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class LocalCreateEdgeSpeedTest extends AbstractGraphBenchmark {
  public LocalCreateEdgeSpeedTest() {
    super("Edges", 100000);
  }

  @Test
  public void test() {

    // TX
    {
      final OrientBaseGraph graph = createGraphTx();

//      step("createDenseGraphTx", new Step() {
//        @Override
//        public void execute(final long items) {
//          createDenseGraph(graph, items);
//        }
//      });

      step("createDenseGraphTxMTx8", new Step() {
        @Override
        public void execute(final long items) {
          executeMultiThreads(8, 100000, new ThreadOperation<OrientBaseGraph>() {
            @Override
            public void execute(final OrientBaseGraph db, final int iThreadId, final long iFirst, final long iLast) {
              final OrientGraph graphThread = new OrientGraph(graph.getRawGraph().getURL());
              graphThread.getRawGraph().getTransaction().setUsingLog(false);
              createDenseGraph(graphThread, iLast - iFirst);
            }
          });
        }
      });
      dropDatabase();
    }

    OGlobalConfiguration.USE_WAL.setValue(false);
    // NO TX
    {
      final OrientBaseGraph graph = createGraphNoTx();

      //      step("createDenseGraphNoTx", new Step() {
      //        @Override
      //        public void execute(final long items) {
      //          createDenseGraph(graph, items);
      //        }
      //      });

      step("createDenseGraphNoTxMTx8", new Step() {
        @Override
        public void execute(final long items) {
          executeMultiThreads(8, 100000, new ThreadOperation<OrientBaseGraph>() {
            @Override
            public void execute(final OrientBaseGraph db, final int iThreadId, final long iFirst, final long iLast) {
              final OrientGraphNoTx graphThread = new OrientGraphNoTx(graph.getRawGraph().getURL());
              createDenseGraph(graphThread, iLast - iFirst);
            }
          });
        }
      });

      dropDatabase();
    }

    end();
  }

  protected void createDenseGraph(final OrientBaseGraph graph, final long items) {
    final OrientVertex v = graph.addVertex(null, "level", 0);

    final AtomicLong totalVertices = new AtomicLong();
    final AtomicLong totalEdges = new AtomicLong();

    addEdges(graph, v, 20, 1, 10, totalVertices, totalEdges, items, Long.MAX_VALUE);
  }

  private void addEdges(final OrientBaseGraph graph, final OrientVertex v, final int iChildren, final int iCurrentLevel,
      final int iMaxLevel, final AtomicLong totalVertices, final AtomicLong totalEdges, final long iMaxVertices,
      final long iMaxEdges) {
    for (int i = 0; i < iChildren; ++i) {
      final OrientVertex child = graph.addVertex(null, "level", iCurrentLevel, "childNum", i);
      totalVertices.incrementAndGet();

      v.addEdge("E", child);
      totalEdges.incrementAndGet();

      if (totalVertices.get() % 5000 == 0)
        OLogManager.instance().info(null, "Populating tree level %d of %d counter=%d maxItems=%d", iCurrentLevel, iMaxLevel,
            totalVertices.get(), iMaxVertices);

      if (totalVertices.get() > iMaxVertices)
        break;

      if (totalEdges.get() > iMaxEdges)
        break;

      if (iCurrentLevel < iMaxLevel) {
        addEdges(graph, child, iChildren, iCurrentLevel + 1, iMaxLevel, totalVertices, totalEdges, iMaxVertices, iMaxEdges);

        if (totalVertices.get() > iMaxVertices)
          break;

        if (totalEdges.get() > iMaxEdges)
          break;
      }
    }
    graph.commit();
  }
}
