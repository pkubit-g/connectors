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

import io.delta.alpine.internal.sources.AlpineHadoopConf
import io.delta.alpine.internal.storage.{HDFSReadOnlyLogStore, ReadOnlyLogStore}
import io.delta.alpine.internal.util.GoldenTableUtils._
import io.delta.alpine.storage.{ReadOnlyLogStore => JReadOnlyLogStore}
import org.apache.commons.io.IOUtils
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
class ReadOnlyLogStoreSuite extends FunSuite {
  // scalastyle:on funsuite
  test("instantiation through hadoopConf - default store") {
    val conf = new Configuration()
    val defaultLogStore = ReadOnlyLogStore.createLogStore(conf)
    assert(defaultLogStore.getClass.getName == ReadOnlyLogStore.defaultLogStoreClassName)
  }

  test("instantiation through hadoopConf - user-defined store") {
    val conf = new Configuration()
    conf.set(AlpineHadoopConf.LOG_STORE_CLASS_KEY, classOf[UserDefinedReadOnlyLogStore].getName)
    val userDefinedLogStore = ReadOnlyLogStore.createLogStore(conf)
    assert(userDefinedLogStore.getClass.getName == classOf[UserDefinedReadOnlyLogStore].getName)
  }

  test("read") {
    withGoldenTable("log-store-read") { tablePath =>
      val conf = new Configuration()
      conf.set(AlpineHadoopConf.LOG_STORE_CLASS_KEY, classOf[HDFSReadOnlyLogStore].getName)
      val readStore = ReadOnlyLogStore.createLogStore(conf)

      val deltas = Seq(0, 1).map(i => new File(tablePath, i.toString)).map(_.getCanonicalPath)
      assert(readStore.read(deltas.head).asScala == Seq("zero", "none"))
      assert(readStore.read(deltas(1)).asScala == Seq("one"))
    }
  }

  test("listFrom") {
    withGoldenTable("log-store-listFrom") { tablePath =>
      val conf = new Configuration()
      conf.set(AlpineHadoopConf.LOG_STORE_CLASS_KEY, classOf[HDFSReadOnlyLogStore].getName)

      val readStore = ReadOnlyLogStore.createLogStore(conf)
      val deltas = Seq(0, 1, 2, 3, 4)
        .map(i => new File(tablePath, i.toString))
        .map(_.toURI)
        .map(new Path(_))

      assert(readStore.listFrom(deltas.head).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(1)).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(2)).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(3)).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(3).map(_.toString))
      assert(readStore.listFrom(deltas(4)).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Nil)
    }
  }
}

/**
 * Sample user-defined log store implementing public Java ABC [[JReadOnlyLogStore]]
 */
class UserDefinedReadOnlyLogStore(hadoopConf: Configuration) extends JReadOnlyLogStore {

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
