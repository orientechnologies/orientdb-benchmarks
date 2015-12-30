package com.orientechnologies.benchmarks;////////////////////////////////////////////////////////////////////////////////

//
//	File Name:	CreationTest.java
//
// Author:		S. Colin Leister (colin)
//
//	Purpose:		
//
// $Revision: $
// =============================================================================
// $Log: $
////////////////////////////////////////////////////////////////////////////////

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CreationTest {
  class NanoTimer {
    private long _StartTime, _StopTime;

    public void Start() {
      _StartTime = System.nanoTime();
    }

    public void Stop() {
      _StopTime = System.nanoTime();
    }

    public long Duration() {
      return _StopTime - _StartTime;
    }

    public String Print() {
      return java.text.NumberFormat.getNumberInstance().format(Duration());
    }
  }

  public static void main(String[] args) throws Exception {
//    OGlobalConfiguration.USE_WAL.setValue(false);
    try {
      CreationTest tt = new CreationTest(args);
    } catch (Exception ex) {
      System.out.println("Something bad happened: " + ex);
    }
  }

  public CreationTest() {
  }

  private final int _NumOfThreads    = 8;
  private final int _NumOfIterations = 100000;

  public CreationTest(String[] args) {
    try {
      if (args.length > 0) {
        if (args[0].equals("help")) {
          System.out.println("java -jar CreationTest.jar");
        }
      }

      CreateDatabase();

      DocThreadTest();

      DropDatabase();
    } catch (Exception ex) {
      System.out.println("CreationTest.CreationTest() Exception: " + ex);
    }
  }

  private void CreateDatabase() {
    // OServerAdmin serverAdmin = new OServerAdmin("remote:localhost:2424").connect("root", "password");
    // serverAdmin.createDatabase("TestDB", "graph", "plocal");

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:./databases/TestDB").create();

    CreateSchema(db);

    db.close();
    // db = new ODatabaseDocumentTx("remote:localhost:2424/TestDB").open("admin", "admin");

    System.out.println("Database creation successful.");
  }

  private void CreateSchema(ODatabaseDocumentTx db) {
    try {
      OClass user = db.getMetadata().getSchema().createClass("User");

      OClass session = db.getMetadata().getSchema().createClass("Session");

      System.out.println("Schema was created successfully.");
    } catch (Exception ex) {
      System.out.println("CreationTest.CreateSchema() Exception: " + ex);
    }
  }

  private void DropDatabase() {
    try {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:./databases/TestDB").open("admin", "admin");
      db.drop();

      System.out.println("Database was dropped successfully.");
    } catch (Exception ex) {
      System.out.println("CreationTest.DropDatabase() Exception: " + ex);
    }
  }

  private void DocumentTest(ODatabaseDocumentTx db, final String clusterPostfix) {
    try {
      NanoTimer nt = new NanoTimer();
      nt.Start();

//       db.begin();

//      db.declareIntent(new OIntentMassiveInsert());

      for (int cnt = 0; cnt < _NumOfIterations; cnt++) {
         db.begin();

        ODocument session1 = new ODocument("Session");
        session1.field("Name", "Irrelevant");
        session1.field("Type", "SomeSession");
        session1.save("Session" + clusterPostfix);

        ODocument session2 = new ODocument("Session");
        session2.field("Name", "Irrelevant");
        session2.field("Type", "SomeSession");
        session2.save("Session" + clusterPostfix);

        ODocument session3 = new ODocument("Session");
        session3.field("Name", "Irrelevant");
        session3.field("Type", "SomeSession");
        session2.save("Session" + clusterPostfix);

        ODocument session4 = new ODocument("Session");
        session4.field("Name", "Irrelevant");
        session4.field("Type", "SomeSession");
        session2.save("Session" + clusterPostfix);

        ODocument session5 = new ODocument("Session");
        session5.field("Name", "Irrelevant");
        session5.field("Type", "SomeSession");
        session2.save("Session" + clusterPostfix);

        List list = new ArrayList();
        list.add(session1);
        list.add(session2);
        list.add(session3);
        list.add(session4);
        list.add(session5);

        ODocument user = new ODocument("User");
        user.field("LastName", "George");
        user.field("FirstName", "Bill");
        user.field("Id", cnt);
        user.field("Description", "Some really long careless string that means nothing and serves no real purpose in life.");
        user.field("SessionList", list, OType.LINKLIST);

        user.save("User" + clusterPostfix);

         db.commit();
      }

      // db.commit();

      nt.Stop();

      System.out.println("CreationTest.DocumentTest() Took " + nt.Print() + " ns");
    } catch (Exception ex) // ORecordDuplicatedException
    {
      System.out.println("CreationTest.DocumentTest() Exception: " + ex);
    }
  }

  private void DocThreadTest() {
    try {
      OPartitionedDatabasePoolFactory pdpf = new OPartitionedDatabasePoolFactory();
      // final OPartitionedDatabasePool dbPool = pdpf.get("remote:localhost/TestDB", "root", "password");
      final OPartitionedDatabasePool dbPool = pdpf.get("plocal:./databases/TestDB", "admin", "admin");

      System.out.println("Beginning Test:");

      /*
       * ODatabaseDocumentTx db = dbPool.acquire();
       * 
       * DocumentTest(db);
       * 
       * db.close();
       */

      final long begin = System.currentTimeMillis();

      ExecutorService es = Executors.newFixedThreadPool(_NumOfThreads);

      NanoTimer nt = new NanoTimer();
      nt.Start();

      for (int cnt = 0; cnt < _NumOfThreads; cnt++) {
        Submit(dbPool, es, cnt == 0 ? "" : "_" + cnt);
      }

      es.shutdown();
      es.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);

      nt.Stop();

      System.out.println("Test took " + (System.currentTimeMillis() - begin) + " ms");

      pdpf.close();
    } catch (Exception ex) {
      System.out.println("DocThreadTest() Exception: " + ex);
    }
  }

  private Future<Integer> Submit(final OPartitionedDatabasePool dbPool, ExecutorService es, final String suffix) {
    Future<Integer> future = es.submit(new Callable<Integer>() {
      public Integer call() {
        int count = 0;

        ODatabaseDocumentTx db = dbPool.acquire();

        DocumentTest(db, suffix);

        db.close();

        return count;
      }
    });

    return future;
  }
}
