package doric
package syntax

import cats.data.Kleisli
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import cats.instances.either.catsParallelForEitherAndValidated
import doric.sem.{ColumnMultyTypeError, Location}
import doric.types.SparkType

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.types.DataType
private[doric] case class MatchType(
    validation: Doric[Column],
    transformation: DoricColumn[_],
    stype: DataType
)

private[doric] case class AnyMatcher[A: SparkType](
    col: DoricColumn[Any],
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
  ): AnyMatcher[A] = {
    val validated: Doric[Column] =
      col.elem.seqFlatMap(x => SparkType[B].validateType(x))

    val transformed = f(validated.toDC)
    this.copy(
      transformations = MatchType(
        validated,
        transformed,
        SparkType[B].dataType
      ) :: transformations
    )
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
      transformations.reverse
        .collectFirst {
          case MatchType(v, t, _) if v.run(df).isValid => t.elem.run(df)
        }
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
      transformations.reverse
        .collectFirst {
          case MatchType(v, t, _) if v.run(df).isValid => t.elem.run(df)
        }
        .fold[DoricValidated[Column]](
          ColumnMultyTypeError(
            transformations.map(_.stype),
            SparkType[A].dataType
          ).invalidNec
        )(identity)
    }.toDC
}

trait AnyColumns {

  implicit class AnyColumnSyntax(x: DoricColumn[Any]) {

    /**
      * Matches depending the type and transform to a common type
      * @tparam A
      *   The final type to transform
      * @return
      *   Builder to specify the possible matches.
      */
    def matchTo[A: SparkType]: AnyMatcher[A] = AnyMatcher(x)
  }

}
