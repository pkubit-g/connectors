/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import java.util.{Collections, Optional, TimeZone, UUID}

import scala.collection.JavaConverters._

import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.{DeltaLog, OptimisticTransaction}
import io.delta.standalone.actions.{Action => ActionJ, Format => FormatJ, Metadata => MetadataJ}
import io.delta.standalone.data.RowRecord
import io.delta.standalone.types.{IntegerType, StructField, StructType}
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class DeltaDataWriterSuite extends FunSuite {
  // scalastyle:on funsuite

  test("basic data write") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val dataSchema = new StructType(Array(new StructField("col1", new IntegerType(), false)))
//      val timeZone = TimeZone.getTimeZone("PST")
//
//      val metadata = new MetadataJ(UUID.randomUUID().toString, null, null, new FormatJ(), null,
//        Collections.emptyList(), Collections.emptyMap(), Optional.of(100L), dataSchema)

      val records = (0 until 10).map { i =>
        RowRecord.empty(dataSchema).add("col1", i)
      }.asJava

      val txn = log.startTransaction()
      // TODO: txn.updateMetadata(metadata);
      txn.writeRecordsAndCommit(records)

      val readData = log.update().open()

      // TODO
      //   readData.asScala.map(row => row.getInt("col1")).toSet
      //   Field "col1" does not exist. Available fields: []
      //   java.lang.IllegalArgumentException: Field "col1" does not exist. Available fields: []

      assert(readData.asScala.length == 10)
    }
  }

  test("another write API") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
//      val dataSchema = new StructType(Array(
//        new StructField("col1", new IntegerType(), false),
//        new StructField("col2", new IntegerType(), false)
//      ))
//      val metadata = new MetadataJ(UUID.randomUUID().toString, null, null, new FormatJ(), null,
//        Collections.emptyList(), Collections.emptyMap(), Optional.of(100L), dataSchema)

      val data = (0 until 10).map { i =>
        val list: java.util.List[Object] = new java.util.ArrayList[Object]()
        list.add(i.asInstanceOf[Object])
        list.add((i * 2).asInstanceOf[Object])
        list
      }.asJava
      val txn = log.startTransaction()

      // TODO: txn.updateMetadata(metadata);
      txn.writeDataAndCommit(data)

      val readData = log.update().open()
      assert(readData.asScala.length == 10)
    }
  }
}
