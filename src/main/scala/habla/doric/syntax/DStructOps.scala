package habla.doric
package syntax

import cats.data.NonEmptyChain

trait DStructOps {

  type DoricEither[A] = Either[NonEmptyChain[Throwable], A]

  implicit class DStructSyntax(private val col: DStructColumn) {
    def getChild[T: FromDf](subColumnName: String): DoricColumn[T] = {
      val subColumnCompleteName: List[String] = col.name :+ subColumnName
      get[T](subColumnCompleteName.mkString(".")).copy(name = subColumnCompleteName)
    }
  }

}
