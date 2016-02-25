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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ReadDocumentSpeedTest extends AbstractDocumentBenchmark {
  public ReadDocumentSpeedTest() {
    super("Documents");
  }

  @Test
  public void test() {
    {
      // NO WAL
      OGlobalConfiguration.USE_WAL.setValue(false);

      final ODatabaseDocumentTx db = createDatabase();

      step("createOneCluster10Props", new Step() {
        @Override
        public void execute(final long items) {
          createOneCluster(db, items, 5, 5, 20);
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
      step("scanOneClusterAccessAllProperties", new Step() {
        @Override
        public void execute(final long items) {
          scanOneClusterAccessAllProperties(db, items);
        }
      });

      step("queryOneClusterReadOneProperty", new Step() {
        @Override
        public void execute(final long items) {
          queryOneClusterReadOneProperty(db, items);
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

      step("queryMultipleClustersReadOneProperty", new Step() {
        @Override
        public void execute(final long items) {
          queryMultipleClustersReadOneProperty(db, items);
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
    Assert.assertEquals(items, total);
  }

  protected void scanOneClusterAccessAllProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("OneCluster")) {
      doc.deserializeFields();
      total++;
    }
    Assert.assertEquals(total, items);
  }

  protected void queryOneClusterReadOneProperty(final ODatabaseDocumentTx db, long items) {
    final List<?> result = db.query(new OSQLSynchQuery<ODocument>("select from OneCluster where name <> 'test'"));
    Assert.assertEquals(0, result.size());
  }

  protected void scanMultipleClusters(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleClusters")) {
      total++;
    }
    Assert.assertEquals(items, total);
  }

  protected void scanMultipleClustersAccessAllProperties(final ODatabaseDocumentTx db, long items) {
    long total = 0;
    for (ODocument doc : db.browseClass("MultipleClusters")) {
      doc.deserializeFields();
      total++;
    }
    Assert.assertEquals(items, total);
  }

  protected void queryMultipleClustersReadOneProperty(final ODatabaseDocumentTx db, long items) {
    final List<?> result = db.query(new OSQLSynchQuery<ODocument>("select from MultipleClusters where name <> 'test'"));
    Assert.assertEquals(result.size(), 0);
  }

  protected void queryMultipleClustersReadSomeProperties(final ODatabaseDocumentTx db, long items) {
    final List<?> result = db.query(new OSQLSynchQuery<ODocument>(
        "select from MultipleClusters where key < 10 or longField0 < 20 or longField1 < 100"));

    Assert.assertEquals(100, result.size());
  }
}
