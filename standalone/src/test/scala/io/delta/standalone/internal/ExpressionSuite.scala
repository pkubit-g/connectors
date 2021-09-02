package io.delta.standalone.internal

// scalastyle:off funsuite
import io.delta.standalone.expressions._
import org.scalatest.FunSuite

class ExpressionSuite extends FunSuite {
  // scalastyle:on funsuite

  test("basic") {
    val expr = new And(
      BooleanLiteral.True,
      BooleanLiteral.False
    )
  }
}
