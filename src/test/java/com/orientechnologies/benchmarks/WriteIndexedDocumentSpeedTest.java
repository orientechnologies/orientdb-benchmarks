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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class WriteIndexedDocumentSpeedTest extends AbstractDocumentBenchmark {
  public WriteIndexedDocumentSpeedTest() {
    super("IndexedDocuments");
  }

  public void test() {
    {
      // USE WAL
      OGlobalConfiguration.USE_WAL.setValue(true);

      final ODatabaseDocumentTx db = createDatabase();

      step("createMultipleClustersIndexedHash8", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createIndex(db, "MultipleClustersIndexedHash8", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
          createMultipleClusters(db, items, "MultipleClustersIndexedHash8", 8, 1, 0, 0);
          db.declareIntent(null);
        }
      });

      step("createMultipleClustersIndexedSBTree8", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createIndex(db, "MultipleClustersIndexedSBTree8", OClass.INDEX_TYPE.UNIQUE);
          createMultipleClusters(db, items, "MultipleClustersIndexedSBTree8", 1, 2, 2, 2);
          db.declareIntent(null);
        }
      });

      step("createMultipleClassesIndexedSBTree1", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createIndexes(db, "MultipleClustersIndexedSBTree1", OClass.INDEX_TYPE.UNIQUE, 1);
          createMultipleClasses(db, items, "MultipleClustersIndexedSBTree1", 1, 1, 0, 0);
          db.declareIntent(null);
        }
      });

      step("createMultipleClassesIndexedSBTree8", new Step() {
        @Override
        public void execute(final long items) {
          db.declareIntent(new OIntentMassiveInsert());
          createIndexes(db, "MultipleClustersIndexedSBTree8", OClass.INDEX_TYPE.UNIQUE, 8);
          createMultipleClasses(db, items, "MultipleClustersIndexedSBTree8", 8, 1, 0, 0);
          db.declareIntent(null);
        }
      });

//      // ENABLE THIS ONLY IN AUTOSHARDING-INDEX BRANCH
//      step("createMultipleClustersIndexedAutoSharding8", new Step() {
//        @Override
//        public void execute(final long items) {
//          db.declareIntent(new OIntentMassiveInsert());
//
//          final OClass cls = db.getMetadata().getSchema().createClass("MultipleClustersIndexedAutoSharding8");
//          cls.createProperty("key", OType.LONG);
//          cls.createIndex("idx_createMultipleClustersIndexedAutoSharding8", OClass.INDEX_TYPE.UNIQUE.toString(),
//              (OProgressListener) null, (ODocument) null, "AUTOSHARDING", new String[] { "key" });
//
//          createMultipleClusters(db, items, "MultipleClustersIndexedAutoSharding8", 8, 1, 0, 0);
//
//          db.declareIntent(null);
//        }
//      });

      dropDatabase();
    }

    end();

  }

  protected void createIndex(final ODatabaseDocumentTx db, final String iClassName, final OClass.INDEX_TYPE iType) {
    final OClass cls = db.getMetadata().getSchema().createClass(iClassName);
    cls.createProperty("key", OType.LONG).createIndex(iType);
  }

  protected void createIndexes(final ODatabaseDocumentTx db, final String iClassName, final OClass.INDEX_TYPE iType,
      final int iTotal) {
    for (int i = 0; i < iTotal; ++i) {
      final OClass cls = db.getMetadata().getSchema().createClass(iClassName + i);
      cls.createProperty("key", OType.LONG).createIndex(iType);
    }
  }

}
