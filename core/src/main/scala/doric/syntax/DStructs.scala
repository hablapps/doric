package doric
package syntax

import scala.language.dynamics
import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.evidence.Is
import cats.implicits._
import doric.sem.{ChildColumnNotFound, ColumnTypeError, DoricSingleError, Location}
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions.{struct => sparkStruct}
import org.apache.spark.sql.types.StructType

private[syntax] trait DStructs {

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
    * @group Struct Type
    * @param cols
    *   the columns that will form the struct
    * @return
    *   A DStruct DoricColumn.
    */
  def struct(cols: DoricColumn[_]*): RowColumn =
    cols.map(_.elem).toList.sequence.map(c => sparkStruct(c: _*)).toDC

  implicit class DStructOps(private val col: RowColumn) {

    /**
      * Retreaves the child row of the Struct column
      * @group Struct Type
      * @param subColumnName
      *   the column name expected to find in the struct.
      * @param location
      *   the location if an error is generated
      * @tparam T
      *   the expected type of the child column.
      * @return
      *   a reference to the child column of the provided type.
      */
    def getChild[T: SparkType](
        subColumnName: String
    )(implicit location: Location): DoricColumn[T] = {
      col.elem
        .mapK(toEither)
        .flatMap(vcolumn =>
          Kleisli[DoricEither, Dataset[_], Column]((df: Dataset[_]) => {
            val fatherColumn = df.select(vcolumn).schema.head
            fatherColumn.dataType match {
              case fatherStructType: StructType =>
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
                        fatherColumn.name + "." + subColumnName,
                        SparkType[T].dataType,
                        st.dataType
                      ).leftNec[Column]
                  )
              case _ => // should not happen
                ColumnTypeError(
                  fatherColumn.name,
                  SparkType[Row].dataType,
                  fatherColumn.dataType
                ).leftNec[Column]
            }
          })
        )
        .mapK(toValidated)
        .toDC
    }
  }

  trait DynamicFieldAccessor[T] extends Dynamic { self: DoricColumn[T] =>

    /**
      * Allows for accessing fields of struct columns using the syntax `rowcol.name[T]`.
      * This expression stands for `rowcol.getChild[T](name)`.
      *
      * @param name
      * @param location
      * @param st
      * @tparam A
      * @return The column which refers to the given field
      * @throws doric.sem.ColumnTypeError if the parent column is not a struct
      */
    def selectDynamic[A](name: String)(implicit
        location: Location,
        st: SparkType[A],
        w: T Is Row
    ): DoricColumn[A] =
      w.lift[DoricColumn].coerce(self).getChild[A](name)
  }

}
