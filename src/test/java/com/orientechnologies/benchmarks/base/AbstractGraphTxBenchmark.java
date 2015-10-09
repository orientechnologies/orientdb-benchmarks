package com.orientechnologies.benchmarks.base;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * @author Luca Garulli
 */
public class AbstractGraphTxBenchmark extends AbstractBenchmark<OrientGraph> {
  private OrientGraph graph;

  protected AbstractGraphTxBenchmark(final String iTestName, final long iTotalItems) {
    super(iTestName, iTotalItems);
  }

  public AbstractGraphTxBenchmark(final String iTestName) {
    super(iTestName);
  }

  @Override
  protected void runInMultiThread(final int iThreadId, final long iThreadFirst, final long iThreadLast,
      final AbstractBenchmark.ThreadOperation<OrientGraph> iThreadOperation) {
    final OrientGraph graph = createGraph();
    try {
      iThreadOperation.execute(graph, iThreadId, iThreadFirst, iThreadLast);
    } finally {
      graph.shutdown();
    }
  }

  @Override
  protected void closeDatabase() {
    if (graph != null)
      graph.shutdown(true);
  }

  @Override
  protected void dropDatabase() {
    if (graph != null)
      graph.drop();
  }

  protected OrientGraph createGraph() {
    if (graph != null)
      throw new IllegalStateException("database already created");

    graph = new OrientGraph(getURL());
    graph.drop();

    graph = new OrientGraph(getURL());
    return graph;
  }

  protected OrientGraph openGraph() {
    return new OrientGraph(getURL());
  }
}
