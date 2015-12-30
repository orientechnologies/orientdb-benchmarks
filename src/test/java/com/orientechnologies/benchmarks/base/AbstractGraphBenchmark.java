package com.orientechnologies.benchmarks.base;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Luca Garulli
 */
public class AbstractGraphBenchmark extends AbstractBenchmark<OrientBaseGraph> {
  private OrientBaseGraph graph;

  protected AbstractGraphBenchmark(final String iTestName, final long iTotalItems) {
    super(iTestName, iTotalItems);
  }

  public AbstractGraphBenchmark(final String iTestName) {
    super(iTestName);
  }

  @Override
  protected void runInMultiThread(final int iThreadId, final long iThreadFirst, final long iThreadLast,
      final AbstractBenchmark.ThreadOperation<OrientBaseGraph> iThreadOperation) {
    try {
      iThreadOperation.execute(graph, iThreadId, iThreadFirst, iThreadLast);
    } finally {
      graph.shutdown();
    }
  }

  @Override
  protected void closeDatabase() {
    if (graph != null)
      if (!graph.isClosed())
        graph.shutdown();
  }

  @Override
  protected void dropDatabase() {
    if (graph != null) {
      if (!graph.isClosed())
        graph.drop();
      graph = null;
    }
  }

  protected OrientGraph createGraphTx() {
    if (graph != null)
      throw new IllegalStateException("database already created");

    graph = new OrientGraph(getURL());
    graph.drop();

    graph = new OrientGraph(getURL());
    return (OrientGraph) graph;
  }

  protected OrientGraphNoTx createGraphNoTx() {
    if (graph != null)
      throw new IllegalStateException("database already created");

    graph = new OrientGraphNoTx(getURL());
    graph.drop();

    graph = new OrientGraphNoTx(getURL());
    return (OrientGraphNoTx) graph;
  }

  protected OrientGraph openGraphTx() {
    return new OrientGraph(getURL());
  }

  protected OrientGraphNoTx openGraphNoTx() {
    return new OrientGraphNoTx(getURL());
  }
}
