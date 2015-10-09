package com.orientechnologies.benchmarks.base;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Luca Garulli
 */
public class AbstractDatabaseBenchmark extends AbstractBenchmark<ODatabaseDocumentTx> {
  private ODatabaseDocumentTx database;

  protected AbstractDatabaseBenchmark(final String iTestName, final long iTotalItems) {
    super(iTestName, iTotalItems);
  }

  public AbstractDatabaseBenchmark(final String iTestName) {
    super(iTestName);
  }

  @Override
  protected void runInMultiThread(final int iThreadId, final long iThreadFirst, final long iThreadLast,
      final ThreadOperation<ODatabaseDocumentTx> iThreadOperation) {
    final ODatabaseDocumentTx db = openDatabase();
    try {
      iThreadOperation.execute(db, iThreadId, iThreadFirst, iThreadLast);
    } finally {
      db.close();
    }
  }

  @Override
  protected void closeDatabase() {
    if (database != null) {
      database.close();
      database = null;
    }
  }

  @Override
  protected void dropDatabase() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  protected ODatabaseDocumentTx createDatabase() {
    if (database != null)
      throw new IllegalStateException("database already created");

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getURL());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
    database = db;
    return db;
  }

  protected ODatabaseDocumentTx openDatabase() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getURL());
    if (db.exists()) {
      db.open("admin", "admin");
    } else
      db.create();
    return db;
  }
}
