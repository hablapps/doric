package habla.doric.syntax

import habla.doric.syntax.TypeColumnHelper.sparkFunction
import habla.doric.{BooleanColumn, FromDf, Literal, ToColumn}

case class NumericOperations[DT]() {

  def +[T: ToColumn: FromDf](column: T, other: T): T = sparkFunction(column, other, _ + _)

  def -[T: ToColumn: FromDf](column: T, other: T): T = sparkFunction(column, other, _ - _)

  def *[T: ToColumn: FromDf](column: T, other: T): T = sparkFunction(column, other, _ * _)

  def >[T: ToColumn: FromDf](column: T, other: T): BooleanColumn =
    sparkFunction[T, BooleanColumn](column, other, _ > _)

  def >=[T: ToColumn: FromDf](column: T, other: T): BooleanColumn =
    sparkFunction[T, BooleanColumn](column, other, _ >= _)

  def <[T: ToColumn: FromDf](column: T, other: T): BooleanColumn =
    sparkFunction[T, BooleanColumn](column, other, _ < _)

  def <=[T: ToColumn: FromDf](column: T, other: T): BooleanColumn =
    sparkFunction[T, BooleanColumn](column, other, _ <= _)

}

trait NumericOperationsOps {

  implicit class NumericOperationsSyntax[T: NumericOperations: ToColumn: FromDf](column: T) {

    type Lit[ST] = Literal[T, ST]

    def +(other: T): T = implicitly[NumericOperations[T]].+(column, other)

    def +[LT: Lit](other: LT): T =
      implicitly[NumericOperations[T]].+(column, implicitly[Lit[LT]].createTLiteral(other))

    def -(other: T): T = implicitly[NumericOperations[T]] - (column, other)

    def -[LT: Lit](other: LT): T = column - implicitly[Lit[LT]].createTLiteral(other)

    def *(other: T): T = implicitly[NumericOperations[T]] * (column, other)

    def *[LT: Lit](other: LT): T = column * implicitly[Lit[LT]].createTLiteral(other)

    def >(other: T): BooleanColumn = implicitly[NumericOperations[T]] > (column, other)

    def >[LT: Lit](other: LT): BooleanColumn = column > implicitly[Lit[LT]].createTLiteral(other)

    def >=(other: T): BooleanColumn = implicitly[NumericOperations[T]] >= (column, other)

    def >=[LT: Lit](other: LT): BooleanColumn = column >= implicitly[Lit[LT]].createTLiteral(other)

    def <(other: T): BooleanColumn = implicitly[NumericOperations[T]] < (column, other)

    def <[LT: Lit](other: LT): BooleanColumn = column < implicitly[Lit[LT]].createTLiteral(other)

    def <=(other: T): BooleanColumn = implicitly[NumericOperations[T]] <= (column, other)

    def <=[LT: Lit](other: LT): BooleanColumn = column <= implicitly[Lit[LT]].createTLiteral(other)
  }

}
