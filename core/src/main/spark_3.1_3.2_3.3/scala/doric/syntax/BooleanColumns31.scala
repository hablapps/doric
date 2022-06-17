package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{functions => f}

private[syntax] trait BooleanColumns31 {

  /**
    * @group Boolean Type
    */
  implicit class BooleanOperationsSyntax31(
      column: DoricColumn[Boolean]
  ) {

    /**
      * Returns null if the condition is true, and throws an exception otherwise.
      *
      * @throws java.lang.RuntimeException if the condition is false
      * @group Boolean Type
      * @see [[org.apache.spark.sql.functions.assert_true(c:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.assert_true]]
      */
    def assertTrue: NullColumn = column.elem.map(f.assert_true).toDC

    /**
      * Returns null if the condition is true; throws an exception with the error message otherwise.
      *
      * @throws java.lang.RuntimeException if the condition is false
      * @group Boolean Type
      * @see [[org.apache.spark.sql.functions.assert_true(c:org\.apache\.spark\.sql\.Column,e:* org.apache.spark.sql.functions.assert_true]]
      */
    def assertTrue(msg: StringColumn): NullColumn =
      (column.elem, msg.elem).mapN(f.assert_true).toDC
  }
}
