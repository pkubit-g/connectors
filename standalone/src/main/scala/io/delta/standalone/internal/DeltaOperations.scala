package io.delta.standalone.internal

import io.delta.standalone.internal.util.JsonUtils

object DeltaOperations {
  /**
   * An operation that can be performed on a Delta table.
   * @param name The name of the operation.
   */
  sealed abstract class Operation(val name: String) {
    val parameters: Map[String, Any]

    lazy val jsonEncodedValues: Map[String, String] = parameters.mapValues(JsonUtils.toJson(_))

    val operationMetrics: Set[String] = Set()

    val userMetadata: Option[String] = None

    /** Whether this operation changes data */
    def changesData: Boolean = false
  }

}
