package doric
package syntax

import doric.types.DoubleC

import org.apache.spark.sql.{functions => f}

private[syntax] trait AggregationColumns31 {

  /**
    * Aggregate function: returns the approximate `percentile` of the numeric column `col` which
    * is the smallest value in the ordered `col` values (sorted from least to greatest) such that
    * no more than `percentage` of `col` values is less than the value or equal to that value.
    *
    * @param percentage each value must be between 0.0 and 1.0.
    * @param accuracy   controls approximation accuracy at the cost of memory. Higher value of accuracy
    *                   yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * @note Support NumericType, DateType and TimestampType since their internal types are all numeric,
    *       and can be easily cast to double for processing.
    * @group Aggregation DoubleC Type
    * @see [[org.apache.spark.sql.functions.percentile_approx]]
    */
  def percentileApprox[T: DoubleC](
      col: DoricColumn[T],
      percentage: Array[Double],
      accuracy: Int
  ): ArrayColumn[T] = {
    require(
      percentage.forall(x => x >= 0.0 && x <= 1.0),
      "Each value of percentage must be between 0.0 and 1.0."
    )
    require(
      accuracy >= 0 && accuracy < Int.MaxValue,
      s"The accuracy provided must be a literal between (0, ${Int.MaxValue}]" +
        s" (current value = $accuracy)"
    )
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC
  }

  /**
    * Aggregate function: returns the approximate `percentile` of the numeric column `col` which
    * is the smallest value in the ordered `col` values (sorted from least to greatest) such that
    * no more than `percentage` of `col` values is less than the value or equal to that value.
    *
    * @param percentage must be between 0.0 and 1.0.
    * @param accuracy   controls approximation accuracy at the cost of memory. Higher value of accuracy
    *                   yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * @note Support NumericType, DateType and TimestampType since their internal types are all numeric,
    *       and can be easily cast to double for processing.
    * @group Aggregation DoubleC Type
    * @see [[org.apache.spark.sql.functions.percentile_approx]]
    */
  def percentileApprox[T: DoubleC](
      col: DoricColumn[T],
      percentage: Double,
      accuracy: Int
  ): DoricColumn[T] = {
    require(
      percentage >= 0.0 && percentage <= 1.0,
      "Percentage must be between 0.0 and 1.0."
    )
    require(
      accuracy >= 0 && accuracy < Int.MaxValue,
      s"The accuracy provided must be a literal between (0, ${Int.MaxValue}]" +
        s" (current value = $accuracy)"
    )
    col.elem
      .map(f.percentile_approx(_, f.lit(percentage), f.lit(accuracy)))
      .toDC
  }
}
