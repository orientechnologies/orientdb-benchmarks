package com.orientechnologies.benchmarks;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.File;

public class PerformanceIndexTest {
  public final static  String DATABASE_PATH = "target/database";
  private static final int    TOT           = 5000000;
  private static final String TYPE_NAME     = "Person";

  public static void main(String[] args) throws Exception {
    new PerformanceIndexTest().run();
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
      long row = 0;
      for (; row < TOT; ++row) {
        final ODocument record = database.newInstance(TYPE_NAME);

        record.setProperty("id", row);
        record.setProperty("name", "Luca" + row);
        record.setProperty("surname", "Skywalker" + row);
        record.setProperty("locali", 10);
        record.setProperty("notes1",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");
        record.setProperty("notes2",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");

        record.save();

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }
}