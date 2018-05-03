package com.orientechnologies.benchmarks;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RandomTestMultiThreads {
  private static final int    CYCLES           = 1_000_000_000;
  private static final int    STARTING_ACCOUNT = 100;
  private static final int    PARALLEL         = Runtime.getRuntime().availableProcessors();
  private static final int    WORKERS          = Runtime.getRuntime().availableProcessors() * 8;
  private static final String DATABASE_PATH    = "target/databases/random";

  private final AtomicLong                      total             = new AtomicLong();
  private final AtomicLong                      totalTransactions = new AtomicLong();
  private final AtomicLong                      mvccErrors        = new AtomicLong();
  private final Random                          rnd               = new Random();
  private final List<OPair<Integer, Exception>> otherErrors       = new ArrayList<>();
  private       OrientDB                        orient;

  @Test
  public void testRandom() {
    OLogManager.instance().info(this, "Executing " + CYCLES + " transactions with %d workers", WORKERS);

    clean();
    final ODatabaseSession database = createSchema();

    populateDatabase(database);

    long begin = System.currentTimeMillis();
    try {

      final Thread[] threads = new Thread[WORKERS];
      for (int i = 0; i < WORKERS; ++i) {
        final int threadId = i;
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            ODatabaseSession db = orient.open("random", "admin", "admin");
            db.begin();

            long totalTransactionInCurrentTx = 0;

            while (true) {
              final long i = total.incrementAndGet();
              if (i >= CYCLES)
                break;

              try {
                final int op = rnd.nextInt(6);

                if (i % 10000 == 0)
                  OLogManager.instance()
                      .info(this, "Operations %d/%d totalTransactionInCurrentTx=%d totalTransactions=%d (thread=%d)", i, CYCLES,
                          totalTransactionInCurrentTx, totalTransactions.get(), threadId);

                OLogManager.instance().debug(this, "Operation %d %d/%d (thread=%d)", op, i, CYCLES, threadId);

                if (op >= 0 && op <= 2) {
                  final int txOps = rnd.nextInt(10);
                  OLogManager.instance().debug(this, "Creating %d transactions (thread=%d)...", txOps, threadId);
                  createTransactions(database, txOps);
                  totalTransactionInCurrentTx += txOps;
                } else if (op >= 3 && op <= 5) {
                  OLogManager.instance().debug(this, "Querying Account records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", rnd.nextInt(100) + 1);

                  final OResultSet result = database.query("select from Account limit :limit", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }

                } else if (op >= 6 && op <= 7) {
                  OLogManager.instance().debug(this, "Querying Transaction records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", rnd.nextInt((int) totalTransactions.get() + 1) + 1);

                  final OResultSet result = database.query("select from Transaction limit :limit", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }

                } else if (op == 8) {
                  OLogManager.instance().debug(this, "Deleting records (thread=%d)...", threadId);
                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op == 9) {
                  OLogManager.instance().debug(this, "Committing (thread=%d)...", threadId);
                  database.commit();

                  totalTransactions.addAndGet(totalTransactionInCurrentTx);
                  totalTransactionInCurrentTx = 0;

                  database.begin();
                }

              } catch (Exception e) {
                if (e instanceof OConcurrentModificationException) {
                  mvccErrors.incrementAndGet();
                  total.decrementAndGet();
                  totalTransactionInCurrentTx = 0;
                } else {
                  otherErrors.add(new OPair<>(threadId, e));
                  OLogManager.instance().error(this, "UNEXPECTED ERROR: " + e, e);
                }

                if (!db.getTransaction().isActive())
                  db.begin();
              }
            }

            try {
              db.commit();
            } catch (Exception e) {
              mvccErrors.incrementAndGet();
            }

            db.close();

          }
        });
        threads[i].start();
      }

      OLogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");

      for (int i = 0; i < WORKERS; ++i) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    } finally {
      database.activateOnCurrentThread();
      database.close();

      System.out.println("Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get()
          + " otherExceptions=" + otherErrors.size());

      for (OPair<Integer, Exception> entry : otherErrors) {
        System.out.println(" = threadId=" + entry.getKey() + " exception=" + entry.getValue());
      }
    }
  }

  private void createTransactions(final ODatabaseSession database, final int txOps) {
    for (long txId = 0; txId < txOps; ++txId) {
      final OVertex tx = database.newVertex("Transaction");
      tx.setProperty("uuid", UUID.randomUUID().toString());
      tx.setProperty("date", new Date());
      tx.setProperty("amount", rnd.nextInt(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private int deleteRecords(final ODatabaseSession database, final int threadId) {
    if (totalTransactions.get() == 0)
      return 0;

    final ORecordIteratorClass<ODocument> iter = database.browseClass("Transaction");

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = rnd.nextInt((int) totalTransactions.get() + 1 / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int deleted = 0;

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final ODocument next = iter.next();

      if (rnd.nextInt(2) == 0) {
        database.delete(next.getIdentity());
        deleted++;
        OLogManager.instance().debug(this, "Deleted record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return deleted;
  }

  private void populateDatabase(final ODatabaseSession database) {
    long begin = System.currentTimeMillis();

    database.begin();

    try {
      for (long row = 0; row < STARTING_ACCOUNT; ++row) {
        final OVertex record = database.newVertex("Account");
        record.setProperty("id", row);
        record.setProperty("name", "Luca" + row);
        record.setProperty("surname", "Skywalker" + row);
        record.setProperty("registered", new Date());
        record.save();
      }

      database.commit();

    } finally {
      database.close();
      OLogManager.instance().info(this, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private ODatabaseSession createSchema() {
    orient = new OrientDB("plocal:" + DATABASE_PATH, OrientDBConfig.defaultConfig());
    orient.create("random", ODatabaseType.PLOCAL);
    final ODatabaseSession database = orient.open("random", "admin", "admin");

    if (!database.getMetadata().getSchema().existsClass("Account")) {
      OClass vertexClass = database.getMetadata().getSchema().getClass("V");
      final OClass accountType = database.getMetadata().getSchema().createClass("Account", vertexClass);
      accountType.createProperty("id", OType.LONG);
      accountType.createProperty("name", OType.STRING);
      accountType.createProperty("surname", OType.STRING);
      accountType.createProperty("registered", OType.DATETIME);

      accountType.createIndex("Account.id", OClass.INDEX_TYPE.UNIQUE, "id");

      final OClass txType = database.getMetadata().getSchema().createClass("Transaction", vertexClass);
      txType.createProperty("uuid", OType.STRING);
      txType.createProperty("date", OType.DATETIME);
      txType.createProperty("amount", OType.DECIMAL);

      txType.createIndex("Transaction.uuid", OClass.INDEX_TYPE.UNIQUE, "uuid");

      OClass edgeClass = database.getMetadata().getSchema().getClass("E");
      final OClass edgeType = database.getMetadata().getSchema().createClass("PurchasedBy", edgeClass);
      edgeType.createProperty("date", OType.DATETIME);
    }
    return database;
  }

  public static void clean() {
    final File dir = new File(DATABASE_PATH);
    OFileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

}