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
      column: Doric[Column],
      colName: String
  ): LeftDoricColumn[T] =
    LeftDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): LeftDoricColumn[T] =
    LeftDoricColumn(doricColumn.elem)
}

object RightDF extends ColGetters[RightDoricColumn] {
  @inline override protected def constructSide[T](
      column: Doric[Column],
      colName: String
  ): RightDoricColumn[T] =
    RightDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): RightDoricColumn[T] =
    RightDoricColumn(doricColumn.elem)
}
