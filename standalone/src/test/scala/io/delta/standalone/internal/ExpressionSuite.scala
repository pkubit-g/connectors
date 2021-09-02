package io.delta.standalone.internal

// scalastyle:off funsuite
import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions._
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

// scalastyle:off println
class ExpressionSuite extends FunSuite {
  // scalastyle:on funsuite

  private def testPredicate(
      predicate: Predicate,
      expectedResult: Boolean,
      record: RowRecord = null) = {
    println(predicate.treeString())
    println(predicate.eval(record))
    assert(predicate.eval(record) == expectedResult)
  }

  test("basic") {
    testPredicate(new And(Literal.False, Literal.False), false)
    testPredicate(new And(Literal.True, Literal.False), false)
    testPredicate(new And(Literal.False, Literal.True), false)
    testPredicate(new And(Literal.True, Literal.True), true)
    testPredicate(new EqualTo(Literal.of(1), Literal.of(1)), true)
    testPredicate(new EqualTo(Literal.of(1), Literal.of(2)), false)
  }

  test("basic partition filter") {
    val add1 = AddFile("1", Map("col1" -> "0", "col2" -> "0"), 0, 0, dataChange = true)
    val add2 = AddFile("2", Map("col1" -> "0", "col2" -> "1"), 0, 0, dataChange = true)
    val add3 = AddFile("3", Map("col1" -> "1", "col2" -> "0"), 0, 0, dataChange = true)
    val add4 = AddFile("4", Map("col1" -> "1", "col2" -> "1"), 0, 0, dataChange = true)

    val partitionSchema = new StructType(Array(
      new StructField("col1", new IntegerType()),
      new StructField("col2", new IntegerType())))

    val partitionFilter = new And(
      new EqualTo(new Column("col1"), Literal.of(0)),
      new EqualTo(new Column("col2"), Literal.of(1))
    )

    val filteredPartitions = DeltaLogImpl.filterFileList(partitionSchema,
      Seq(add1, add2, add3, add4), partitionFilter :: Nil)

    println(partitionFilter.treeString())
    assert(filteredPartitions.contains(add2))
  }
}
