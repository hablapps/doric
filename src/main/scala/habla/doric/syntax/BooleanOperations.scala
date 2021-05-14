package habla.doric
package syntax

import habla.doric.syntax.TypeColumnHelper.sparkFunction

trait BooleanOperations[T] {

  def and(b1: DoricColumn[T], b2: DoricColumn[T]): DoricColumn[T] =
    sparkFunction(b1, b2, _ && _)
  def or(b1: DoricColumn[T], b2: DoricColumn[T]): DoricColumn[T] =
    sparkFunction(b1, b2, _ || _)
}

object BooleanOperations {
  @inline def apply[T: BooleanOperations]: BooleanOperations[T] =
    implicitly[BooleanOperations[T]]
}

trait BooleanOperationsOps {

  implicit class BooleanOperationsSyntax[T: BooleanOperations: FromDf](
      column: DoricColumn[T]
  ) {
    def and(other: DoricColumn[T]): DoricColumn[T] =
      BooleanOperations[T].and(column, other)
    def &&(other: DoricColumn[T]): DoricColumn[T] =
      BooleanOperations[T].and(column, other)
    def or(other: DoricColumn[T]): DoricColumn[T] =
      BooleanOperations[T].or(column, other)
    def ||(other: DoricColumn[T]): DoricColumn[T] =
      BooleanOperations[T].or(column, other)
  }
}
