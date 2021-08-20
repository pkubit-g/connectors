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

import java.io.File

import io.delta.standalone.data.{CloseableIterator => CloseableIteratorJ}
import io.delta.standalone.storage.{LogStore => LogStoreJ}
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.storage.{HDFSLogStore, LogStore, LogStoreAdaptor, LogStoreProvider}
import io.delta.standalone.internal.util.GoldenTableUtils._
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

// scalastyle:off funsuite
import org.scalatest.FunSuite

/**
 * Instead of using Spark in this project to WRITE data and log files for tests, we have
 * io.delta.golden.GoldenTables do it instead. During tests, we then refer by name to specific
 * golden tables that that class is responsible for generating ahead of time. This allows us to
 * focus on READING only so that we may fully decouple from Spark and not have it as a dependency.
 *
 * See io.delta.golden.GoldenTables for documentation on how to ensure that the needed files have
 * been generated.
 */
abstract class LogStoreSuiteBase extends FunSuite with LogStoreProvider {
  // scalastyle:on funsuite

  def logStoreClassName: Option[String]

  def hadoopConf: Configuration = {
    val conf = new Configuration()
    if (logStoreClassName.isDefined) {
      conf.set(StandaloneHadoopConf.LOG_STORE_CLASS_KEY, logStoreClassName.get)
    }
    conf
  }

  test("instantiation") {
    val expectedClassName = logStoreClassName.getOrElse(LogStore.defaultLogStoreClassName)
    assert(createLogStore(hadoopConf).getClass.getName == expectedClassName)
  }

  test("read") {
    withGoldenTable("log-store-read") { tablePath =>
      val logStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1).map(i => new File(tablePath, i.toString)).map(_.getCanonicalPath)
      assert(logStore.read(deltas.head) == Seq("zero", "none"))
      assert(logStore.read(deltas(1)) == Seq("one"))
    }
  }

  test("listFrom") {
    withGoldenTable("log-store-listFrom") { tablePath =>
      val logStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1, 2, 3, 4)
        .map(i => new File(tablePath, i.toString))
        .map(_.toURI)
        .map(new Path(_))

      assert(logStore.listFrom(deltas.head).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(1)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(2)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(3)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(3).map(_.toString))
      assert(logStore.listFrom(deltas(4)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Nil)
    }
  }
}

/**
 * Test providing a system-defined (standalone.internal.storage) LogStore.
 */
class HDFSLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[HDFSLogStore].getName)
}

/**
 * Test not providing a LogStore classname, in which case [[LogStoreProvider]] will use
 * the default value.
 */
class DefaultLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = None
}

/**
 * Test having the user provide their own LogStore.
 */
class UserDefinedLogStoreSuite extends LogStoreSuiteBase {
  // The actual type of LogStore created will be LogStoreAdaptor.
  override def logStoreClassName: Option[String] = Some(classOf[LogStoreAdaptor].getName)

  override def hadoopConf: Configuration = {
    val conf = new Configuration()
    conf.set(StandaloneHadoopConf.LOG_STORE_CLASS_KEY, classOf[UserDefinedLogStore].getName)
    conf
  }
}

/**
 * Sample user-defined log store implementing [[LogStoreJ]]
 */
class UserDefinedLogStore(override val initHadoopConf: Configuration)
  extends LogStoreJ(initHadoopConf) {

  private val mockImpl = new HDFSLogStore(initHadoopConf)

  override def read(path: Path, hadoopConf: Configuration): CloseableIteratorJ[String] = {
    val iter = mockImpl.read(path).iterator
    new CloseableIteratorJ[String] {
      override def close(): Unit = {}
      override def hasNext: Boolean = iter.hasNext
      override def next(): String = iter.next
    }
  }

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    mockImpl.write(path, actions.asScala, overwrite)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    mockImpl.listFrom(path).asJava
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    mockImpl.resolvePathOnPhysicalStorage(path)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    mockImpl.isPartialWriteVisible(path)
  }
}