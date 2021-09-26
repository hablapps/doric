package doric
package syntax

import cats.data.NonEmptyChain
import cats.implicits.catsSyntaxValidatedId
import doric.sem.{ColumnValidationError, Location}

import org.apache.spark.sql.{Column, Dataset}

trait Actions {
  implicit class AcctionsOps[T](doric: DoricColumn[T]) {
    def action: DoricActions[T] = DoricActions(doric)
  }
}

case class DoricActions[T] private (doric: DoricColumn[T]) {
  def validate: DoricValidations[T] = DoricValidations(doric)
}

case class DoricValidations[T] private (doric: DoricColumn[T]) {
  def all(
      validation: DoricColumn[T] => DoricColumn[Boolean],
      validations: DoricColumn[T] => DoricColumn[Boolean]*
  )(implicit location: Location): DoricColumn[T] =
    Doric[Column]((df: Dataset[_]) => {

      val validationsList = (validation :: validations.toList)
        .map(_(doric))
      val validatedColumn = validationsList.map(x =>
        sum(when[Long].caseW(x, 1L.lit).otherwise(0L.lit))
      )
      val validationsValues = df
        .select(count("*") :: validatedColumn: _*).head
      val validationsNameAndCount =
        df.select(validationsList:_*).columns.zipWithIndex
          .map(x => (x._1, validationsValues.getLong(x._2 + 1)))
          .toList
      val total = validationsValues.getLong(0)
      val invalid =
        NonEmptyChain.fromSeq(validationsNameAndCount.filter(_._2 != total))
      invalid.fold(doric.elem.run(df)) {
        _.map { case (validationName, valid) =>
          ColumnValidationError(
            s"Not all rows passed the validation $validationName ($valid of $total were valid)"
          )
        }.invalid
      }
    }).toDC

  def noneNull(implicit location: Location): DoricColumn[T] = {
    all(_.isNotNull)
  }
}
