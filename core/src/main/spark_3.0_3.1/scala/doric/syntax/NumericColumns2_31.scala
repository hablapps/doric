package doric
package syntax

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.catalyst.expressions.{Expression, RoundBase, ShiftLeft, ShiftRight, ShiftRightUnsigned}

import scala.math.BigDecimal.RoundingMode.RoundingMode

protected trait NumericColumns2_31 {

  /**
    * INTEGRAL OPERATIONS
    */
  implicit class IntegralOperationsSyntax2_31[T: IntegralType](
      column: DoricColumn[T]
  ) {

    /**
      * Shift the given value numBits left.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftLeft]]
      */
    def shiftLeft(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftLeft(c.expr, n.expr)))
        .toDC

    /**
      * (Signed) shift the given value numBits right.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftRight]]
      */
    def shiftRight(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftRight(c.expr, n.expr)))
        .toDC

    /**
      * Unsigned shift the given value numBits right.
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.shiftRightUnsigned]]
      */
    def shiftRightUnsigned(numBits: IntegerColumn): DoricColumn[T] =
      (column.elem, numBits.elem)
        .mapN((c, n) => new Column(ShiftRightUnsigned(c.expr, n.expr)))
        .toDC

    /**
      * Computes bitwise NOT (~) of a number.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.bitwiseNOT]]
      */
    def bitwiseNot: DoricColumn[T] = column.elem.map(f.bitwiseNOT).toDC
  }

  /**
    * NUM WITH DECIMALS OPERATIONS
    */
  implicit class NumWithDecimalsOperationsSyntax2_31[T: NumWithDecimalsType](
      column: DoricColumn[T]
  ) {

    /**
      * DORIC EXCLUSIVE! Round the value to `scale` decimal places with given round `mode`
      * if `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
      *
      * @todo decimal type
      * @group Numeric Type
      */
    def round(scale: IntegerColumn, mode: RoundingMode): DoricColumn[T] = {
      case class DoricRound(
          child: Expression,
          scale: Expression,
          mode: RoundingMode
      ) extends RoundBase(child, scale, mode, s"ROUND_$mode")

      (column.elem, scale.elem)
        .mapN((c, s) => new Column(DoricRound(c.expr, s.expr, mode)))
        .toDC
    }
  }

}
