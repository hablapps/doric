package mrpowers.bebe

import mrpowers.bebe.ColumnSyntax.sparkFunction

case class NumericOperations[DT]() {
  def +[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ + _)

  def -[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ - _)

  def *[T: ToColumn : FromDf](column: T, other: T): T = sparkFunction(column, other, _ * _)

  def >[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ > _)

  def >=[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ >= _)

  def <[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ < _)

  def <=[T: ToColumn : FromDf](column: T, other: T): BooleanColumn = sparkFunction[T, BooleanColumn](column, other, _ <= _)

}

trait ArithmeticExtras {

  implicit class ArithmeticOps[T: NumericOperations : ToColumn : FromDf](column: T) {
    def +(other: T): T = implicitly[NumericOperations[T]].+(column, other)

    def -(other: T): T = implicitly[NumericOperations[T]].-(column, other)

    def *(other: T): T = implicitly[NumericOperations[T]].*(column, other)

    def >(other: T): BooleanColumn = implicitly[NumericOperations[T]].>(column, other)

    def >=(other: T): BooleanColumn = implicitly[NumericOperations[T]].>=(column, other)

    def <(other: T): BooleanColumn = implicitly[NumericOperations[T]].<(column, other)

    def <=(other: T): BooleanColumn = implicitly[NumericOperations[T]].<=(column, other)
  }

}
