package doric

import cats.data.Kleisli
import cats.implicits.catsSyntaxTuple2Semigroupal

import org.apache.spark.sql.{Column, Dataset}

sealed abstract class JoinSideDoricColumn[T] protected (
    val elem: Doric[Column]
) {
  type OtherSide <: JoinSideDoricColumn[T]
  def ===(otherElem: OtherSide): DoricJoinColumn = {
    val isLeft = this.isInstanceOf[LeftDoricColumn[T]]

    Kleisli[DoricValidated, (Dataset[_], Dataset[_]), Column](dfs => {
      (
        elem.run(dfs._1).asSideDfError(isLeft),
        otherElem.elem.run(dfs._2).asSideDfError(!isLeft)
      ).mapN(_ === _)
    }).toDJC
  }
}

case class LeftDoricColumn[T](override val elem: Doric[Column])
    extends JoinSideDoricColumn[T](elem) {
  override type OtherSide = RightDoricColumn[T]
}

case class RightDoricColumn[T](override val elem: Doric[Column])
    extends JoinSideDoricColumn[T](elem) {
  override type OtherSide = LeftDoricColumn[T]
}
