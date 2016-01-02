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
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

public class WriteDocumentSpeedTest extends AbstractDocumentBenchmark {
  public WriteDocumentSpeedTest() {
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
          createOneCluster(db, items, 5, 5, 20);
        }
      });
      step("createMultipleCluster", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClusters(db, items, 8, 1, 0, 0);
        }
      });
      step("createMultipleClusters4MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 4, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters8MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 8, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters8MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 8, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters16MassiveInsert", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 16, 1, 0, 0);
          db.declareIntent(null);
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
          createOneCluster(db, items, 5, 5, 20);
        }
      });
      step("createMultipleClusterNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          createMultipleClusters(db, items, 8, 1, 0, 0);
        }
      });
      step("createMultipleClusters4MassiveInsertNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 4, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters8MassiveInsertNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 8, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters16MassiveInsertNoWAL", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 16, 1, 0, 0);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters16MassiveInsertNoWAL10Props", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 16, 5, 5, 20);
          db.declareIntent(null);
        }
      });
      step("createMultipleClusters16MassiveInsertNoWAL50BigProps", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createMultipleClusters(db, items, 16, 25, 25, 40);
          db.declareIntent(null);
        }
      });

      OGlobalConfiguration.USE_WAL.setValue(true);

    }

    end();

  }
}
