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

import org.junit.Assert;

import com.orientechnologies.benchmarks.base.DatabaseAbstractBenchmark;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class LocalCreateDocumentSpeedTest extends DatabaseAbstractBenchmark {
  public LocalCreateDocumentSpeedTest() {
    super("Documents");
  }

  public void test() {
    {
      // USE WAL
      OGlobalConfiguration.USE_WAL.setValue(true);

      final ODatabaseDocumentTx db = createDatabase();

      step("createOneCluster", new Step() {
        @Override
        public void execute(final long items) {
          createOneCluster(db, items);
        }
      });
      step("createMultipleCluster", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleCluster(db, items);
        }
      });
      step("createMultipleClusters8MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClustersMassiveInsert(db, items, 8);
        }
      });
      step("createMultipleClusters16MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClustersMassiveInsert(db, items, 16);
        }
      });

      dropDatabase();
    }

    {
      // NO WAL

      OGlobalConfiguration.USE_WAL.setValue(false);
      final ODatabaseDocumentTx db = createDatabase();

      step("createOneClusterNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          createOneCluster(db, items);
        }
      });
      step("createMultipleClusterNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleCluster(db, items);
        }
      });
      step("createMultipleClusters8MassiveInsertNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClustersMassiveInsert(db, items, 8);
        }
      });
      step("createMultipleClusters16MassiveInsertNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClustersMassiveInsert(db, items, 16);
        }
      });
      OGlobalConfiguration.USE_WAL.setValue(true);

      // SCAN ONE CLUSTER

      // WARMUP
      scanOneCluster(db, getTotalItems());

      step("scanOneCluster", new Step() {
        @Override
        public void execute(final long items) {
          scanOneCluster(db, items);
        }
      });

      step("scanOneClusterAccessProperties", new Step() {
        @Override
        public void execute(final long items) {
          scanOneClusterAccessProperties(db, items);
        }
      });

      step("queryOneCluster", new Step() {
        @Override
        public void execute(final long items) {
          queryOneCluster(db, items);
        }
      });

      step("queryOneClusterAccessProperties", new Step() {
        @Override
        public void execute(final long items) {
          queryOneClusterAccessProperties(db, items);
        }
      });

      // SCAN MULTIPLE CLUSTERS

      // WARMUP
      scanMultipleCluster(db, getTotalItems());

      step("scanMultipleCluster", new Step() {
        @Override
        public void execute(final long items) {
          scanMultipleCluster(db, items);
        }
      });

      step("scanMultipleClusterAccessProperties", new Step() {
        @Override
        public void execute(final long items) {
          scanMultipleClusterAccessProperties(db, items);
        }
      });

      step("queryMultipleCluster", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleCluster(db, items);
        }
      });

      step("queryMultipleClusterAccessProperties", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClusterAccessProperties(db, items);
        }
      });
    }

    end();

  }

  protected void createOneCluster(final ODatabaseDocumentTx db, final long items) {
    db.getMetadata().getSchema().createClass("OneCluster");
    for (int i = 0; i < items; ++i) {
      new ODocument("OneCluster").fields("name", "test", "key", i).save();
    }
  }

  protected void createMultipleCluster(final ODatabaseDocumentTx db, final long items) {
    final OClass cls = db.getMetadata().getSchema().createClass("MultipleCluster");
    OClassImpl.addClusters(cls, getConcurrencyLevel());

    executeMultiThreads(getConcurrencyLevel(), items, new ThreadOperation<ODatabaseDocumentTx>() {
      @Override
      public void execute(final ODatabaseDocumentTx db, final int iThreadId, final long iFirst, final long iLast) {
        for (long i = iFirst; i <= iLast; ++i) {
          new ODocument("MultipleCluster").fields("name", "test", "key", i).save();
        }
      }
    });
  }

  protected void createMultipleClustersMassiveInsert(final ODatabaseDocumentTx db, final long items, final int iConcurrencyLevel) {
    final String clsName = "MultipleClusters" + iConcurrencyLevel + "MassiveInsert";

    final OClass cls = db.getMetadata().getSchema().createClass(clsName);
    OClassImpl.addClusters(cls, iConcurrencyLevel);

    db.declareIntent(new OIntentMassiveInsert());
    executeMultiThreads(iConcurrencyLevel, items, new ThreadOperation<ODatabaseDocumentTx>() {
      @Override
      public void execute(final ODatabaseDocumentTx db, final int iThreadId, final long iFirst, final long iLast) {
        for (long i = iFirst; i <= iLast; ++i) {
          new ODocument(clsName).fields("name", "test", "key", i).save();
        }
      }
    });
  }

  protected void scanOneCluster(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("OneCluster")) {
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void scanOneClusterAccessProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("OneCluster")) {
      doc.deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryOneCluster(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (Object doc : db.query(new OSQLSynchQuery<ODocument>("select from OneCluster"))) {
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryOneClusterAccessProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (Object doc : db.query(new OSQLSynchQuery<ODocument>("select from OneCluster"))) {
      ((ODocument) doc).deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void scanMultipleCluster(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleCluster")) {
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void scanMultipleClusterAccessProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleCluster")) {
      doc.deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryMultipleCluster(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (Object doc : db.query(new OSQLSynchQuery<ODocument>("select from MultipleCluster"))) {
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryMultipleClusterAccessProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (Object doc : db.query(new OSQLSynchQuery<ODocument>("select from MultipleCluster"))) {
      ((ODocument) doc).deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }
}
