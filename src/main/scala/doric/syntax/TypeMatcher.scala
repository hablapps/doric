package doric
package syntax

import cats.data.Kleisli
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import doric.sem.{ColumnMultyTypeError, Location}
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType

private[doric] case class MatchType(
    validation: Doric[Column],
    transformation: DoricColumn[_],
    stype: DataType
)

private[doric] case class EmptyTypeMatcher[A: SparkType](
    columnName: String
) {

  /**
    * If the column matches de type, applies the transformation
    * @param f
    *   the function to apply if the type matches
    * @tparam B
    *   the type to match the column
    * @return
    *   The builder to continue to configure
    */
  def caseType[B: SparkType](
      f: DoricColumn[B] => DoricColumn[A]
  ): NonEmptyTypeMatcher[A] = {
    val validated: Doric[Column] =
      col[B](columnName).elem

    val transformed = f(validated.toDC)
    NonEmptyTypeMatcher(
      columnName = columnName,
      transformations = List(
        MatchType(
          validated,
          transformed,
          SparkType[B].dataType
        )
      )
    )
  }
}

private[doric] case class NonEmptyTypeMatcher[A: SparkType](
    columnName: String,
    transformations: List[MatchType] = List.empty
) {

  /**
    * If the column matches de type, applies the transformation
    * @param f
    *   the function to apply if the type matches
    * @tparam B
    *   the type to match the column
    * @return
    *   The builder to continue to configure
    */
  def caseType[B: SparkType](
      f: DoricColumn[B] => DoricColumn[A]
  ): NonEmptyTypeMatcher[A] = {
    val validated: Doric[Column] =
      col[B](columnName).elem

    val transformed = f(validated.toDC)
    this.copy(
      transformations = MatchType(
        validated,
        transformed,
        SparkType[B].dataType
      ) :: transformations
    )
  }

  def getFirstValid(df: Dataset[_]): Option[DoricValidated[Column]] =
    transformations.reverse
      .collectFirst {
        case MatchType(v, t, _) if v.run(df).isValid => t.elem.run(df)
      }

  /**
    * In case that any of the types matches, applies this column value.
    * @param default
    *   the column to put as default value
    * @return
    *   Doric column of the expected type
    */
  def inOtherCase(default: DoricColumn[A]): DoricColumn[A] =
    Kleisli[DoricValidated, Dataset[_], Column] { df =>
      getFirstValid(df)
        .fold(default.elem.run(df))(identity)
    }.toDC

  /**
    * In case that any of the types matches, applies this column value.
    * @param location
    *   marks this point as the error location
    * @return
    *   The doric column specified
    */
  def inOtherCaseError(implicit location: Location): DoricColumn[A] =
    Kleisli[DoricValidated, Dataset[_], Column] { df =>
      getFirstValid(df)
        .fold[DoricValidated[Column]](
          ColumnMultyTypeError(
            columnName,
            transformations.map(_.stype),
            SparkType[A].dataType
          ).invalidNec
        )(identity)
    }.toDC
}

trait TypeMatcher {

  def matchToType[T: SparkType](colName: String): EmptyTypeMatcher[T] =
    EmptyTypeMatcher[T](colName)

}
