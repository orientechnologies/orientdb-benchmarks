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

import com.orientechnologies.benchmarks.base.AbstractDocumentBenchmark;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicLong;

public class ReadDocumentSpeedTest extends AbstractDocumentBenchmark {
  public ReadDocumentSpeedTest() {
    super("Documents");
  }

  public void test() {
    {
      // NO WAL
      OGlobalConfiguration.USE_WAL.setValue(false);

      final ODatabaseDocumentTx db = createDatabase();

      step("createOneCluster10Props", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClusters(db, items, 8, 5, 5, 20);
        }
      });
      step("createMultipleClusters50BigProps", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClusters(db, items, 8, 25, 25, 40);
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
      scanMultipleClusters(db, getTotalItems());

      step("scanMultipleClusters", new Step() {
        @Override
        public void execute(final long items) {
          scanMultipleClusters(db, items);
        }
      });
      step("scanMultipleClustersAccessAllProperties", new Step() {
        @Override
        public void execute(final long items) {
          scanMultipleClustersAccessAllProperties(db, items);
        }
      });
      step("queryMultipleClusters", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClusters(db, items);
        }
      });
      step("queryMultipleClustersAccessAllProperties", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClustersAccessAllProperties(db, items);
        }
      });

      step("queryMultipleClustersCountOneProperty", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClustersCountOneProperty(db, items);
        }
      });
      step("queryMultipleClustersReadSomeProperties", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClustersReadSomeProperties(db, items);
        }
      });

    }

    end();

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
    final AtomicLong total = new AtomicLong(0);
    db.query(new OSQLAsynchQuery<ODocument>("select from OneCluster", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        total.incrementAndGet();
        return true;
      }

      @Override
      public void end() {
      }

      @Override
      public Object getResult() {
        return null;
      }
    }));

    Assert.assertEquals(total.longValue(), items);
  }

  protected void queryOneClusterAccessProperties(final ODatabaseDocumentTx db, long items) {
    final AtomicLong total = new AtomicLong(0);
    db.query(new OSQLAsynchQuery<ODocument>("select from OneCluster", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        ((ODocument) iRecord).deserializeFields();
        total.incrementAndGet();
        return true;
      }

      @Override
      public void end() {
      }

      @Override
      public Object getResult() {
        return null;
      }
    }));

    Assert.assertEquals(total.longValue(), items);
  }

  protected void scanMultipleClusters(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleClusters")) {
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void scanMultipleClustersAccessAllProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleClusters")) {
      doc.deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryMultipleClusters(final ODatabaseDocumentTx db, long items) {
    final AtomicLong total = new AtomicLong(0);
    db.query(new OSQLAsynchQuery<ODocument>("select from MultipleClusters", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        total.incrementAndGet();
        return true;
      }

      @Override
      public void end() {
      }

      @Override
      public Object getResult() {
        return null;
      }
    }));

    Assert.assertEquals(total.longValue(), items);
  }

  protected void queryMultipleClustersAccessAllProperties(final ODatabaseDocumentTx db, long items) {
    final AtomicLong total = new AtomicLong(0);
    db.query(new OSQLAsynchQuery<ODocument>("select from MultipleClusters", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        ((ODocument) iRecord).deserializeFields();
        total.incrementAndGet();
        return true;
      }

      @Override
      public void end() {
      }

      @Override
      public Object getResult() {
        return null;
      }
    }));

    Assert.assertEquals(total.longValue(), items);
  }

  protected void queryMultipleClustersCountOneProperty(final ODatabaseDocumentTx db, long items) {
    final AtomicLong total = new AtomicLong(0);
    db.query(new OSQLAsynchQuery<ODocument>("select from MultipleClusters where name = 'test'", new OCommandResultListener() {
      @Override
      public boolean result(Object iRecord) {
        ((ODocument) iRecord).deserializeFields();
        total.incrementAndGet();
        return true;
      }

      @Override
      public void end() {
      }

      @Override
      public Object getResult() {
        return null;
      }
    }));

    Assert.assertEquals(total.longValue(), items);
  }

  protected void queryMultipleClustersReadSomeProperties(final ODatabaseDocumentTx db, long items) {
    final AtomicLong total = new AtomicLong(0);
    final String str = createStringValue(10);
    db.query(new OSQLAsynchQuery<ODocument>(
        "select from MultipleClusters where longValue0 < 10 or longValue1 < 1000 or stringField0 <> '" + str + "Z'",
        new OCommandResultListener() {
          @Override
          public boolean result(Object iRecord) {
            ((ODocument) iRecord).deserializeFields();
            total.incrementAndGet();
            return true;
          }

          @Override
          public void end() {
          }

          @Override
          public Object getResult() {
            return null;
          }
        }));

    Assert.assertEquals(total.longValue(), items);
  }
}
