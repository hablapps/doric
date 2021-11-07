package doric
package syntax

import cats.implicits.catsSyntaxTuple2Semigroupal
import doric.DoricColumn.sparkFunction
import doric.types.NumericType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.FormatNumber

private[syntax] trait NumericColumns {

  implicit class NumericOperationsSyntax[T: NumericType](
      column: DoricColumn[T]
  ) {

    /**
      * @group Numeric Type
      */
    def +(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ + _)

    /**
      * @group Numeric Type
      */
    def -(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ - _)

    /**
      * @group Numeric Type
      */
    def *(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ * _)

    /**
      * @group Numeric Type
      */
    def /(other: DoricColumn[T]): DoricColumn[Double] =
      sparkFunction(column, other, _ / _)

    /**
      * @group Numeric Type
      */
    def %(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ % _)

    /**
      * @group Comparable Type
      */
    def >(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ > _)

    /**
      * @group Comparable Type
      */
    def >=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ >= _)

    /**
      * @group Comparable Type
      */
    def <(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ < _)

    /**
      * @group Comparable Type
      */
    def <=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ <= _)

    /**
      * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places
      * with HALF_EVEN round mode, and returns the result as a string column.
      *
      * If d is 0, the result has no decimal point or fractional part.
      * If d is less than 0, the result will be null.
      *
      * @group Numeric Type
      */
    def formatNumber(decimals: IntegerColumn): StringColumn =
      (column.elem, decimals.elem)
        .mapN((c, d) => {
          new Column(FormatNumber(c.expr, d.expr))
        })
        .toDC

  }

}
