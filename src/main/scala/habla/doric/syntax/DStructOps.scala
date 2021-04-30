package habla.doric
package syntax

import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType

trait DStructOps {

  type DoricEither[A] = Either[NonEmptyChain[DoricSingleError], A]

  private val toValidated = new FunctionK[DoricEither, DoricValidated] {
    override def apply[A](fa: DoricEither[A]): DoricValidated[A] =
      fa.toValidated
  }
  private val toEither = new FunctionK[DoricValidated, DoricEither] {
    override def apply[A](fa: DoricValidated[A]): DoricEither[A] = fa.toEither
  }

  implicit class DStructSyntax(private val col: DStructColumn) {
    def getChild[T: FromDf](
        subColumnName: String
    )(implicit location: Location): DoricColumn[T] = {
      col.elem
        .mapK(toEither)
        .flatMap(vcolumn =>
          Kleisli[DoricEither, DataFrame, Column]((df: DataFrame) => {
            val fatherColumn = df.select(vcolumn).schema.head
            val fatherStructType = fatherColumn.dataType
              .asInstanceOf[StructType]
            fatherStructType
              .find(_.name == subColumnName)
              .fold[DoricEither[Column]](
                ChildColumnNotFound(
                  subColumnName,
                  fatherStructType.names
                ).leftNec
              )(st =>
                if (FromDf[T].isValid(st.dataType))
                  vcolumn
                    .getItem(subColumnName)
                    .asRight[NonEmptyChain[DoricSingleError]]
                else
                  ColumnTypeError(
                    subColumnName,
                    FromDf[T].dataType,
                    st.dataType
                  ).leftNec[Column]
              )
          })
        )
        .mapK(toValidated)
        .toDC
    }
  }

}
