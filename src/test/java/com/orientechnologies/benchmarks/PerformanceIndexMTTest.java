package com.orientechnologies.benchmarks;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceIndexMTTest {
  public final static  String    DATABASE_PATH = "target/database";
  private static final int       TOT           = 5000000;
  private static final String    TYPE_NAME     = "Person";
  private static final ODocument END           = new ODocument();

  public static void main(String[] args) throws Exception {
    new PerformanceIndexMTTest().run();
  }

  private void run() {
    final File dir = new File(DATABASE_PATH + "/performance");
    OFileUtils.deleteRecursively(dir);
    dir.mkdirs();

    final int parallel = 2;

    OrientDB orient = new OrientDB("plocal:" + DATABASE_PATH, OrientDBConfig.defaultConfig());
    orient.create("performance", ODatabaseType.PLOCAL);
    final ODatabaseSession database = orient.open("performance", "admin", "admin");

    if (!database.getMetadata().getSchema().existsClass(TYPE_NAME)) {
      final OClass personType = database.getMetadata().getSchema().createClass(TYPE_NAME, parallel);
      personType.createProperty("id", OType.LONG);
      personType.createProperty("name", OType.STRING);
      personType.createProperty("surname", OType.STRING);
      personType.createProperty("locali", OType.INTEGER);
      personType.createProperty("notes1", OType.STRING);
      personType.createProperty("notes2", OType.STRING);

      personType.createIndex(TYPE_NAME + ".id", OClass.INDEX_TYPE.UNIQUE, "id");
    }

    long begin = System.currentTimeMillis();

    try {

      final Thread[] threads = new Thread[parallel];
      final ArrayBlockingQueue<ODocument>[] queues = new ArrayBlockingQueue[parallel];
      final AtomicInteger[] counter = new AtomicInteger[parallel];

      for (int i = 0; i < parallel; ++i) {
        queues[i] = new ArrayBlockingQueue<ODocument>(1024);
        counter[i] = new AtomicInteger();

        final int id = i;
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            final ODatabaseSession database = orient.open("performance", "admin", "admin");
            while (true) {
              try {
                ODocument record = queues[id].take();

                if (record == END)
                  break;

                database.save(record, TYPE_NAME + (id == 0 ? "" : "_" + id));

                counter[id].incrementAndGet();

              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            OLogManager.instance().info(this, "Closing thread %d", id);
            database.close();
            OLogManager.instance().info(this, "Closed thread %d", id);
          }
        });
        threads[i].start();
      }

      long row = 0;
      for (; row < TOT; ++row) {
        final ODocument record = database.newInstance(TYPE_NAME);

        record.setProperty("id", row);
        record.setProperty("name", "Luca" + row);
        record.setProperty("surname", "Skywalker" + row);
        record.setProperty("locali", 10);
        record.setProperty("notes1",
            "This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields.");
        record.setProperty("notes2",
            "This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields. This is a long field to check how OrientDB behaves with large fields.");

        try {
          queues[(int) (row % parallel)].put(record);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      }

      for (int i = 0; i < parallel; ++i) {
        try {
          queues[i].put(END);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      for (int i = 0; i < parallel; ++i) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      System.out.println(" Counters ");
      for (int i = 0; i < parallel; ++i)
        System.out.println(" - " + counter[i].get());

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }
}