package habla.doric
package syntax

import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType

trait DStructOps {

  type DoricEither[A] = Either[NonEmptyChain[Throwable], A]

  val toValidated = new FunctionK[DoricEither, DoricValidated] {
    override def apply[A](fa: DoricEither[A]): DoricValidated[A] = fa.toValidated
  }
  val toEither = new FunctionK[DoricValidated, DoricEither] {
    override def apply[A](fa: DoricValidated[A]): DoricEither[A] = fa.toEither
  }

  implicit class DStructSyntax(private val col: DStructColumn) {
    def getChild[T: FromDf](subColumnName: String): DoricColumn[T] = {
      col.elem
        .mapK(toEither)
        .flatMap(vcolumn =>
          Kleisli[DoricEither, DataFrame, Column]((df: DataFrame) => {
            val fatherColumn = df.select(vcolumn).schema.head
            fatherColumn.dataType
              .asInstanceOf[StructType]
              .find(_.name == subColumnName)
              .fold[DoricEither[Column]](
                new Exception(
                  s"Column '$subColumnName' is not a child of '${fatherColumn.name}'"
                ).leftNec
              )(st =>
                if (FromDf[T].isValid(st.dataType))
                  vcolumn.getItem(subColumnName).asRight[NonEmptyChain[Throwable]]
                else
                  new Exception(
                    s"The nested column ${subColumnName} is of type ${st.dataType} and it was expected to be ${FromDf[T].dataType}"
                  ).leftNec[Column]
              )
          })
        )
        .mapK(toValidated)
        .toDC
    }
  }

}
