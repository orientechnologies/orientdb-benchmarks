package com.orientechnologies.benchmarks.base;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Luca Garulli
 */
public class AbstractGraphNoTxBenchmark extends AbstractBenchmark<OrientGraphNoTx> {
  private OrientGraphNoTx graph;

  protected AbstractGraphNoTxBenchmark(final String iTestName, final long iTotalItems) {
    super(iTestName, iTotalItems);
  }

  public AbstractGraphNoTxBenchmark(final String iTestName) {
    super(iTestName);
  }

  @Override
  protected void runInMultiThread(final int iThreadId, final long iThreadFirst, final long iThreadLast,
      final ThreadOperation<OrientGraphNoTx> iThreadOperation) {
    final OrientGraphNoTx graph = openGraph();
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

  protected OrientGraphNoTx createGraph() {
    if (graph != null)
      throw new IllegalStateException("database already created");

    graph = new OrientGraphNoTx(getURL());
    graph.drop();

    graph = new OrientGraphNoTx(getURL());
    return graph;
  }

  protected OrientGraphNoTx openGraph() {
    return new OrientGraphNoTx(getURL());
  }
}
