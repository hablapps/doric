package doric
package syntax

import doric.DoricColumn.sparkFunction
import doric.types.NumericType

trait NumericColumnSyntax {

  implicit class NumericOperationsSyntax[T: NumericType](
      column: DoricColumn[T]
  ) {

    def +(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ + _)

    def -(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ - _)

    def *(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ * _)

    def >(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ > _)

    def >=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ >= _)

    def <(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ < _)

    def <=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ <= _)

  }

}
