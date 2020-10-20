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

package io.delta.alpine.internal

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.stream.Collectors

import collection.JavaConverters._

import io.delta.alpine.internal.storage.{HDFSReadOnlyLogStore, ReadOnlyLogStoreProvider}
import io.delta.alpine.sources.AlpineHadoopConf
import io.delta.alpine.ReadOnlyLogStore
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

// TODO private[internal] ??
abstract class ReadOnlyLogStoreSuiteBase extends QueryTest
    with ReadOnlyLogStoreProvider
    with SharedSparkSession {

  def logStoreClassName: Option[String]

  def hadoopConf: Configuration = {
    val conf = new Configuration()
    if (logStoreClassName.isDefined) {
      conf.set(AlpineHadoopConf.LOG_STORE_CLASS_KEY, logStoreClassName.get)
    }
    conf
  }

  test("instantiation through hadoopConf") {
    if (logStoreClassName.isDefined) {
      assert(hadoopConf.get(AlpineHadoopConf.LOG_STORE_CLASS_KEY) == logStoreClassName.get)
      assert(createLogStore(hadoopConf).getClass.getName == logStoreClassName.get)
    } else {
      assert(createLogStore(hadoopConf).getClass.getName ==
        ReadOnlyLogStoreProvider.defaultLogStoreClassName)
    }
  }

  test("read") {
    withTempDir { tempDir =>
      val writeStore = LogStore(spark.sparkContext)
      val readStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
      writeStore.write(deltas.head, Iterator("zero", "none"))
      writeStore.write(deltas(1), Iterator("one"))

      assert(readStore.read(deltas.head).asScala == Seq("zero", "none"))
      assert(readStore.read(deltas(1)).asScala == Seq("one"))
    }
  }

  test("listFrom") {
    withTempDir { tempDir =>
      val writeStore = LogStore(spark.sparkContext)
      val readStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1, 2, 3, 4)
        .map(i => new File(tempDir, i.toString))
        .map(_.toURI)
        .map(new Path(_))

      writeStore.write(deltas(1), Iterator("zero"))
      writeStore.write(deltas(2), Iterator("one"))
      writeStore.write(deltas(3), Iterator("two"))
      assert(readStore.listFrom(deltas.head).asScala.map(_.getPath.getName).toArray ===
        Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(1)).asScala.map(_.getPath.getName).toArray ===
        Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(2)).asScala.map(_.getPath.getName).toArray ===
        Seq(2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(3)).asScala.map(_.getPath.getName).toArray ===
        Seq(3).map(_.toString))
      assert(readStore.listFrom(deltas(4)).asScala.map(_.getPath.getName).toArray === Nil)
    }
  }
}

/**
 * Test providing a system-defined (alpine.internal.storage) log store
 */
class HDFSReadOnlyLogStoreSuite extends ReadOnlyLogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[HDFSReadOnlyLogStore].getName)
}

/**
 * Test not providing a log store class name, in which case [[ReadOnlyLogStoreProvider]] will use
 * the default value
 */
class DefaultReadOnlyLogStoreSuite extends ReadOnlyLogStoreSuiteBase {
  override def logStoreClassName: Option[String] = None
}

/**
 * Test providing a user-defined log store
 */
class UserDefinedReadOnlyLogStoreSuite extends ReadOnlyLogStoreSuiteBase {
  override def logStoreClassName: Option[String] =
    Some(classOf[UserDefinedReadOnlyLogStore].getName)
}

/**
 * Sample user-defined log store implementing public Java ABC [[ReadOnlyLogStore]]
 */
class UserDefinedReadOnlyLogStore(hadoopConf: Configuration) extends ReadOnlyLogStore {

  override def read(path: Path): java.util.List[String] = {
    val fs = path.getFileSystem(hadoopConf)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).stream().map[String](x => x.trim)
        .collect(Collectors.toList[String])
    } finally {
      stream.close()
    }
  }

  override def listFrom(path: Path): java.util.Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator.asJava
  }
}
