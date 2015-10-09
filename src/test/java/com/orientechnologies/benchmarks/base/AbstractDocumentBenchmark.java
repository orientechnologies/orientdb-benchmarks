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
package com.orientechnologies.benchmarks.base;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class AbstractDocumentBenchmark extends AbstractDatabaseBenchmark {
  public AbstractDocumentBenchmark(String iName) {
    super(iName);
  }

  protected void createOneCluster(final ODatabaseDocumentTx db, final long items, final int iLongFields, final int iStringFields,
      final int iStringSize) {
    db.getMetadata().getSchema().createClass("OneCluster");

    final String strValue = createStringValue(iStringSize);
    for (int i = 0; i < items; ++i) {
      final ODocument doc = createDocument("OneCluster", i, iLongFields, iStringFields, strValue);
      doc.save();
    }
  }

  protected void createMultipleClusters(final ODatabaseDocumentTx db, final long items, final int iConcurrencyLevel,
      final int iLongFields, final int iStringFields, final int iStringSize) {
    createMultipleClusters(db, items, "MultipleClusters", iConcurrencyLevel, iLongFields, iStringFields, iStringSize);
  }

  protected void createMultipleClusters(final ODatabaseDocumentTx db, final long items, final String iClassName,
      final int iConcurrencyLevel, final int iLongFields, final int iStringFields, final int iStringSize) {
    final OClass cls = db.getMetadata().getSchema().createClass(iClassName);
    OClassImpl.addClusters(cls, iConcurrencyLevel);

    executeMultiThreads(iConcurrencyLevel, items, new ThreadOperation<ODatabaseDocumentTx>() {
      @Override
      public void execute(final ODatabaseDocumentTx db, final int iThreadId, final long iFirst, final long iLast) {
        final String strValue = createStringValue(iStringSize);

        for (long i = iFirst; i <= iLast; ++i) {
          final ODocument doc = createDocument(iClassName, i, iLongFields, iStringFields, strValue);
          doc.save();
        }
      }
    });
  }

  protected ODocument createDocument(String clsName, long i, int iLongFields, int iStringFields, String strValue) {
    final ODocument doc = new ODocument(clsName);
    doc.fields("name", "test", "key", i);
    for (int f = 0; f < iLongFields; f++)
      doc.field("longField" + f, f);

    for (int f = 0; f < iStringFields; f++)
      doc.field("stringField" + f, strValue);
    return doc;
  }

  protected String createStringValue(final int iStringSize) {
    final StringBuilder str = new StringBuilder();
    for (int f = 0; f < iStringSize; f++) {
      final String v = "" + f;
      str.append(v.substring(v.length() - 1));
    }
    return str.toString();
  }
}
