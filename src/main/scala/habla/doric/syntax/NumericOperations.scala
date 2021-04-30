package habla.doric
package syntax

import habla.doric.syntax.TypeColumnHelper.sparkFunction

trait NumericOperations[T] {

  def +(column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] =
    sparkFunction(column, other, _ + _)

  def -(column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] =
    sparkFunction(column, other, _ - _)

  def *(column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] =
    sparkFunction(column, other, _ * _)

  def >(column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ > _)

  def >=(column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ >= _)

  def <(column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ < _)

  def <=(column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ <= _)

}

object NumericOperations {
  @inline def apply[T: NumericOperations]: NumericOperations[T] =
    implicitly[NumericOperations[T]]
}

trait NumericOperationsOps {

  implicit class NumericOperationsSyntax[T: NumericOperations: FromDf](
      column: DoricColumn[T]
  ) {

    def +(other: DoricColumn[T]): DoricColumn[T] =
      implicitly[NumericOperations[T]].+(column, other)

    def -(other: DoricColumn[T]): DoricColumn[T] =
      NumericOperations[T] - (column, other)

    def *(other: DoricColumn[T]): DoricColumn[T] =
      NumericOperations[T] * (column, other)

    def >(other: DoricColumn[T]): BooleanColumn =
      NumericOperations[T] > (column, other)

    def >=(other: DoricColumn[T]): BooleanColumn =
      NumericOperations[T] >= (column, other)

    def <(other: DoricColumn[T]): BooleanColumn =
      NumericOperations[T] < (column, other)

    def <=(other: DoricColumn[T]): BooleanColumn =
      NumericOperations[T] <= (column, other)

  }

}
