package doric
package syntax

import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.implicits._
import doric.sem.{ChildColumnNotFound, ColumnTypeError, DoricSingleError, Location}
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{struct => sparkStruct}
import org.apache.spark.sql.types.StructType

trait DStructs {

  private type DoricEither[A] = Either[NonEmptyChain[DoricSingleError], A]

  private val toValidated = new FunctionK[DoricEither, DoricValidated] {
    override def apply[A](fa: DoricEither[A]): DoricValidated[A] =
      fa.toValidated
  }
  private val toEither = new FunctionK[DoricValidated, DoricEither] {
    override def apply[A](fa: DoricValidated[A]): DoricEither[A] = fa.toEither
  }

  /**
    * Creates a struct with the columns
    * @param cols the columns that will form the struct
    * @return A DStruct DoricColumn.
    */
  def struct(cols: DoricColumn[_]*): DStructColumn =
    cols.map(_.elem).toList.sequence.map(c => sparkStruct(c: _*)).toDC

  implicit class DStructOps(private val col: DStructColumn) {

    /**
      * Retreaves the child row of the Struct column
      * @param subColumnName the column name expected to find in the struct.
      * @param location the location if an error is generated
      * @tparam T the expected type of the child column.
      * @return a reference to the child column of the provided type.
      */
    def getChild[T: SparkType](
        subColumnName: String
    )(implicit location: Location): DoricColumn[T] = {
      col.elem
        .mapK(toEither)
        .flatMap(vcolumn =>
          Kleisli[DoricEither, Dataset[_], Column]((df: Dataset[_]) => {
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
                if (SparkType[T].isEqual(st.dataType))
                  vcolumn
                    .getItem(subColumnName)
                    .asRight[NonEmptyChain[DoricSingleError]]
                else
                  ColumnTypeError(
                    subColumnName,
                    SparkType[T].dataType,
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
