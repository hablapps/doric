package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{Expression, RoundBase, ShiftLeft, ShiftRight, ShiftRightUnsigned}

import scala.math.BigDecimal.RoundingMode.RoundingMode

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

  /**
    * NUM WITH DECIMALS OPERATIONS
    */
  implicit class NumWithDecimalsOperationsSyntax32[T: NumWithDecimalsType](
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
      ) extends RoundBase(child, scale, mode, s"ROUND_$mode") {
        override protected def withNewChildrenInternal(
            newLeft: Expression,
            newRight: Expression
        ): DoricRound =
          copy(child = newLeft, scale = newRight)
      }

      (column.elem, scale.elem)
        .mapN((c, s) => new Column(DoricRound(c.expr, s.expr, mode)))
        .toDC
    }
  }

}
