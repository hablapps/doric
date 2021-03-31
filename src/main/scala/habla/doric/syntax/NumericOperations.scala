package habla.doric
package syntax

import habla.doric.syntax.TypeColumnHelper.sparkFunction
import habla.doric.{BooleanColumn, FromDf, Literal}

case class NumericOperations[DT]() {

  def +[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] = sparkFunction(column, other, _ + _)

  def -[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] = sparkFunction(column, other, _ - _)

  def *[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): DoricColumn[T] = sparkFunction(column, other, _ * _)

  def >[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ > _)

  def >=[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ >= _)

  def <[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ < _)

  def <=[T: FromDf](column: DoricColumn[T], other: DoricColumn[T]): BooleanColumn =
    sparkFunction[T, Boolean](column, other, _ <= _)

}

trait NumericOperationsOps {

  implicit class NumericOperationsSyntax[T: NumericOperations: FromDf](column: DoricColumn[T]) {

    type Lit[ST] = Literal[T, ST]

    def +(other: DoricColumn[T]): DoricColumn[T] = implicitly[NumericOperations[T]].+(column, other)

    def +[LT: Lit](other: LT): DoricColumn[T] =
      implicitly[NumericOperations[T]].+(column, implicitly[Lit[LT]].createTLiteral(other))

    def -(other: DoricColumn[T]): DoricColumn[T] = implicitly[NumericOperations[T]] - (column, other)

    def -[LT: Lit](other: LT): DoricColumn[T] = column - implicitly[Lit[LT]].createTLiteral(other)

    def *(other: DoricColumn[T]): DoricColumn[T] = implicitly[NumericOperations[T]] * (column, other)

    def *[LT: Lit](other: LT): DoricColumn[T] = column * implicitly[Lit[LT]].createTLiteral(other)

    def >(other: DoricColumn[T]): BooleanColumn = implicitly[NumericOperations[T]] > (column, other)

    def >[LT: Lit](other: LT): BooleanColumn = column > implicitly[Lit[LT]].createTLiteral(other)

    def >=(other: DoricColumn[T]): BooleanColumn = implicitly[NumericOperations[T]] >= (column, other)

    def >=[LT: Lit](other: LT): BooleanColumn = column >= implicitly[Lit[LT]].createTLiteral(other)

    def <(other: DoricColumn[T]): BooleanColumn = implicitly[NumericOperations[T]] < (column, other)

    def <[LT: Lit](other: LT): BooleanColumn = column < implicitly[Lit[LT]].createTLiteral(other)

    def <=(other: DoricColumn[T]): BooleanColumn = implicitly[NumericOperations[T]] <= (column, other)

    def <=[LT: Lit](other: LT): BooleanColumn = column <= implicitly[Lit[LT]].createTLiteral(other)
  }

}
