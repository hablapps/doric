package doric
package syntax

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import doric.sem.{ColumnValidationError, Location}

import org.apache.spark.sql.{Column, Dataset}

trait Actions {
  implicit class AcctionsOps[T](doric: DoricColumn[T]) {
    def action: DoricActions[T] = DoricActions(doric)
  }
}

case class DoricActions[T] private (doric: DoricColumn[T]) {
  def validation: DoricValidations[T] = DoricValidations(doric)
}

case class DoricValidations[T] private (doric: DoricColumn[T]) {
  def all(
      validation: DoricColumn[T] => DoricColumn[Boolean]
  )(implicit location: Location): DoricColumn[T] =
    Doric[Column]((df: Dataset[_]) => {
      import df.sparkSession.implicits._
      val validatedColumn = validation(doric)
      val (valid, total) = df
        .collectCols(
          sum(when[Long].caseW(validatedColumn, 1L.lit).otherwise(0L.lit)),
          count("*")
        )
        .head
      if (valid == total)
        doric.elem.run(df)
      else {
        val validationName = df.select(validatedColumn).columns.head
        ColumnValidationError(
          s"Not all rows passed the validation $validationName ($valid of $total were valid)"
        ).invalidNec
      }
    }).toDC

  def noneNull(implicit location: Location): DoricColumn[T] = {
    all(_.isNotNull)
  }
}
