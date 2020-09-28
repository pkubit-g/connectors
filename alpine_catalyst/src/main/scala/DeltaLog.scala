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

package main.scala

import java.util.concurrent.locks.ReentrantLock

import main.scala.actions.AddFile
import main.scala.storage.ReadOnlyLogStoreProvider
import main.scala.util.{Clock, SystemClock}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
class DeltaLog private(
  val hadoopConf: Configuration,
  val logPath: Path,
  val dataPath: Path,
  val clock: Clock)
  extends Checkpoints
    with ReadOnlyLogStoreProvider
    with SnapshotManagement {

  private def tombstoneRetentionMillis: Long = 1000000000L // TODO TOMBSTONE_RETENTION

  def minFileRetentionTimestamp: Long = clock.getTimeMillis() - tombstoneRetentionMillis

  lazy val store = createLogStore(hadoopConf)

  protected val deltaLogLock = new ReentrantLock()

  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }
}

object DeltaLog {
  def forTable(hadoopConf: Configuration, dataPath: String): DeltaLog = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  // TODO: forTable w dataPath: File
  def forTable(hadoopConf: Configuration, dataPath: Path): DeltaLog = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }
  // TODO: forTable w dataPath: String & clock
  // TODO: forTable w dataPath: File & clock
  // TODO: forTable w dataPath: Path & clock
  // TODO: forTable w tableName: TableIdentifier
  // TODO: forTable w table: CatalogTable
  // TODO: forTable w tableName: TableIdentifier & clock
  // TODO: forTable w table: CatalogTable & clock
  // TODO: forTable w deltaTable: DeltaTableIdentifier

  def apply(hadoopConf: Configuration, rawPath: Path, clock: Clock = new SystemClock): DeltaLog = {
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)

    new DeltaLog(hadoopConf, path, path.getParent, new SystemClock)
  }

  def filterFileList(
      partitionSchema: StructType,
      files: Seq[AddFile],
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[AddFile] = {
//    /**
//     * TODO
//     * Notes
//     * Analysis: ensures that cols line up (for queryplans); so that
//     * all LogicalPlan expressions are valid
//     * - here, we do NOT have a logical plan. we have an expression taht we want to apply
//     * - expression: is x == 2 ? for it to be evaluated, it needs to be
//     * binded (row: column values); all the row knows is that these are the vals;
//     * it doesn't know the field names
//     *
//     * binding maps the row values (row at some index) to their corresponding column names
//     *
//     * BindReferences:
//     * for a given input row, what is the sequence of column names?
//     */
    val resolver = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution

    val rewrittenFilters = rewritePartitionFilters(
      partitionSchema,
      resolver,
      partitionFilters,
      partitionColumnPrefixes)

    // the expression that we need to bindD
    // it needs to be provided the same schema (since that is the input)
    // this is UNRESOLVED
    // how do you resolve an unresolved expression w/o spark.sql?
    // TODO ask burak

//    val columnFilter = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true))
//    val columnFilter = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true))

    // TODO: no clue what is wrong here
    // scalastyle:off nonascii
    val columnFilter = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true))
    // scalastyle:on nonascii
//    /**
//     * spark.sql.Encoders are spark's version of serialization and des. java types to SQL types
//     * at row level, processing engine doesn't understand java objects. only understands internal
//     * format
//     * thus need a serde between POJO
//     *
//     * product? scala interface called product; lets you create a serde for scala classes
//     * product[Foo].schema is the structType describing the new spark catalyst typesß
//     */

    val encoder = Encoders.product[AddFile]
    val schema = encoder.schema
    val attributesList =
      schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()).toSeq

    val resolvedFilter = columnFilter.transform {
      case UnresolvedAttribute(nameParts) =>
        attributesList.resolve(nameParts, resolver).get
    }

    // TODO create an attribute sequence from the schema
    val boundFilter = BindReferences.bindReference(resolvedFilter, attributesList)

    // take input with some schema into an internal row with that schema
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val foo = files.filter { file =>
      val row = converter(file).asInstanceOf[InternalRow]
      try {
        boundFilter.eval(row).asInstanceOf[Boolean]
      } catch {
        case e: Exception =>
          e.printStackTrace
          throw e
      }
    }
    val x = 5
    foo
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }
}
