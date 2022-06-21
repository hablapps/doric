package doric
package syntax

import scala.language.dynamics

import cats.data.Kleisli
import cats.evidence.Is
import cats.implicits._
import doric.sem.{ColumnTypeError, Location, SparkErrorWrapper}
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.ExtractValue
import org.apache.spark.sql.functions.{struct => sparkStruct}

private[syntax] trait DStructs {

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
      *
      * @group Struct Type
      * @param subColumnName
      * the column name expected to find in the struct.
      * @param location
      * the location if an error is generated
      * @tparam T
      * the expected type of the child column.
      * @return
      * a reference to the child column of the provided type.
      */
    def getChild[T: SparkType](
        subColumnName: String
    )(implicit location: Location): DoricColumn[T] = {
      (col.elem, subColumnName.lit.elem)
        .mapN((a, b) => (a, b))
        .mapK(toEither)
        .flatMap { case (vcolumn, litVal) =>
          Kleisli[DoricEither, Dataset[_], Column]((df: Dataset[_]) => {
            try {
              if (SparkType[Row].isEqual(vcolumn.expr.dataType)) {
                val subColumn = new Column(
                  ExtractValue(
                    vcolumn.expr,
                    litVal.expr,
                    df.sparkSession.sessionState.analyzer.resolver
                  )
                )
                if (SparkType[T].isEqual(subColumn.expr.dataType))
                  subColumn.asRight
                else
                  ColumnTypeError(
                    subColumnName,
                    SparkType[T].dataType,
                    subColumn.expr.dataType
                  ).leftNec
              } else {
                ColumnTypeError(
                  "",
                  SparkType[Row].dataType,
                  vcolumn.expr.dataType
                ).leftNec
              }
            } catch {
              case e: Throwable =>
                SparkErrorWrapper(e).leftNec
            }
          })
        }
        .mapK(toValidated)
        .toDC
    }
  }
  trait DynamicFieldAccessor[T] extends Dynamic { self: DoricColumn[T] =>

    /**
      * Allows for accessing fields of struct columns using the syntax `rowcol.name[T]`.
      * This expression stands for `rowcol.getChild[T](name)`.
      *
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
