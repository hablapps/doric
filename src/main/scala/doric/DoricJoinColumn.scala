package doric

import cats.implicits.catsSyntaxTuple2Semigroupal
import doric.syntax.ColGetters

import org.apache.spark.sql.Column

case class DoricJoinColumn(elem: DoricJoin[Column]) {
  def &&(other: DoricJoinColumn): DoricJoinColumn =
    (elem, other.elem).mapN(_ && _).toDJC
}

object LeftDF extends ColGetters[LeftDoricColumn] {
  @inline override protected def constructSide[T](
      column: Doric[Column]
  ): LeftDoricColumn[T] =
    LeftDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): LeftDoricColumn[T] =
    constructSide(doricColumn.elem)
}

object RightDF extends ColGetters[RightDoricColumn] {
  @inline override protected def constructSide[T](
      column: Doric[Column]
  ): RightDoricColumn[T] =
    RightDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): RightDoricColumn[T] =
    constructSide(doricColumn.elem)
}
