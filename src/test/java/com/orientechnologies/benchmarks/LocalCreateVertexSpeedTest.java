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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Test;

public class LocalCreateVertexSpeedTest extends AbstractGraphBenchmark {
  public LocalCreateVertexSpeedTest() {
    super("Vertices");
  }

  @Test
  public void test() {
    final OrientBaseGraph graph = createGraphNoTx();

    step("createOneCluster", new Step() {
      @Override
      public void execute(final long items) {
        createOneCluster(graph, items);
      }
    });

    step("createMultipleCluster", new Step() {
      @Override
      public void execute(final long items) {
        createMultipleCluster(graph, items);
      }
    });

    step("createMultipleClusterMassiveInsert", new Step() {
      @Override
      public void execute(final long items) {
        createMultipleClusterMassiveInsert(graph, items);
      }
    });

    end();
  }

  protected void createOneCluster(final OrientBaseGraph graph, final long items) {
    graph.createVertexType("OneCluster");
    for (int i = 0; i < items; ++i) {
      graph.addVertex("class:OneCluster", "name", "test", "key", i);
    }
  }

  protected void createMultipleCluster(final OrientBaseGraph graph, final long items) {
    final OClass cls = graph.createVertexType("MultipleCluster");
    // OClassImpl.addClusters(cls, 8);

    for (int i = 0; i < items; ++i) {
      graph.addVertex("class:MultipleCluster", "name", "test", "key", i);
    }
  }

  protected void createMultipleClusterMassiveInsert(final OrientBaseGraph graph, final long items) {
    final OClass cls = graph.createVertexType("MultipleClusterMassiveInsert");
    // OClassImpl.addClusters(cls, 8);

    for (int i = 0; i < items; ++i) {
      graph.addVertex("class:MultipleClusterMassiveInsert", "name", "test", "key", i);
    }
  }
}
