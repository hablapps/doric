package mrpowers.bebe.syntax

import mrpowers.bebe.{BooleanColumn, FromDf, ToColumn}
import mrpowers.bebe.syntax.TypeColumnHelper.sparkFunction

case class NumericOperations[DT]() {

  def +[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ + _)

  def -[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ - _)

  def *[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ * _)

  def >[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ > _)

  def >=[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ >= _)

  def <[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ < _)

  def <=[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ <= _)

}

trait NumericOperationsOps {

  implicit class NumericOperationsSyntax[T: NumericOperations : ToColumn : FromDf](column: T) {

    def +(other: T): T = implicitly[NumericOperations[T]] + (column, other)

    def -(other: T): T = implicitly[NumericOperations[T]] - (column, other)

    def *(other: T): T = implicitly[NumericOperations[T]] * (column, other)

    def >(other: T): BooleanColumn = implicitly[NumericOperations[T]] > (column, other)

    def >=(other: T): BooleanColumn = implicitly[NumericOperations[T]] >= (column, other)

    def <(other: T): BooleanColumn = implicitly[NumericOperations[T]] < (column, other)

    def <=(other: T): BooleanColumn = implicitly[NumericOperations[T]] <= (column, other)
  }

}
