package com.orientechnologies.benchmarks;

import com.orientechnologies.benchmarks.base.AbstractDatabaseBenchmark;
import com.orientechnologies.benchmarks.base.AbstractDocumentBenchmark;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by tglman on 25/02/16.
 */

public class WriteDocumentRemoteSpeedTest extends AbstractDocumentBenchmark {

  private OServer server;

  public WriteDocumentRemoteSpeedTest() {
    super("WriteDocumentRemoteSpeedTest", 1000000);
  }

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {
    server = new OServer(false);
    server.startup(this.getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OServerAdmin serverAdmin = new OServerAdmin(getURL());
    serverAdmin.connect("root", "root");
    serverAdmin.createDatabase(getName(), "plocal");
    serverAdmin.close();
  }

  @After
  public void after() throws IOException {
    OServerAdmin serverAdmin = new OServerAdmin(getURL());
    serverAdmin.connect("root", "root");
    serverAdmin.dropDatabase(getName(), "plocal");
    serverAdmin.close();
    server.shutdown();
    server = null;
  }

  @Test
  public void test() {

    final ODatabaseDocument db = new ODatabaseDocumentTx(getURL());
    db.open("admin", "admin");
    step("createOneCluster", new Step() {
      @Override
      public void execute(long items) {
        createOneCluster(db, items, 5, 5, 20);
      }
    });
    step("createMultipleClusters4", new Step() {
      @Override
      public void execute(final long items) {
        createMultipleClusters(db, items, 4, 1, 0, 0);
      }
    });
    step("createMultipleClusters8", new Step() {
      @Override
      public void execute(final long items) {
        createMultipleClusters(db, items, 8, 1, 0, 0);
      }
    });
    step("createMultipleClusters16", new Step() {
      @Override
      public void execute(final long items) {
        createMultipleClusters(db, items, 16, 1, 0, 0);
      }
    });
    db.close();
    end();
  }

  @Override
  protected ODatabaseDocumentTx openDatabase() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getURL());
    db.open("admin", "admin");
    return db;
  }

  @Override
  protected String getURL() {
    return System.getProperty("url", "remote:localhost/" + getName());
  }
}
