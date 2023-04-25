package doric
package syntax

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.catalyst.expressions.{ShiftLeft, ShiftRight, ShiftRightUnsigned}

protected trait NumericColumns32 {

  /**
    * INTEGRAL OPERATIONS
    */
  implicit class IntegralOperationsSyntax32[T: IntegralType](
      column: DoricColumn[T]
  ) {

    /**
      * Shift the given value numBits left.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftleft]]
      */
    def shiftLeft(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftLeft(c.expr, n.expr)))
        .toDC

    /**
      * (Signed) shift the given value numBits right.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftright]]
      */
    def shiftRight(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftRight(c.expr, n.expr)))
        .toDC

    /**
      * Unsigned shift the given value numBits right.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftrightunsigned]]
      */
    def shiftRightUnsigned(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftRightUnsigned(c.expr, n.expr)))
        .toDC

    /**
      * Computes bitwise NOT (~) of a number.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.bitwise_not]]
      */
    def bitwiseNot: DoricColumn[T] = column.elem.map(f.bitwise_not).toDC
  }

}
