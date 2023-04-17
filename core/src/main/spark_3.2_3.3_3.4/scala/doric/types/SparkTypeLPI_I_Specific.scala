package doric
package types

import org.apache.spark.sql.types.{DayTimeIntervalType, YearMonthIntervalType}

trait SparkTypeLPI_I_Specific {
  self: SparkType.type =>

  implicit val fromDuration: Primitive[java.time.Duration] =
    SparkType[java.time.Duration](DayTimeIntervalType())

  implicit val fromPeriod: Primitive[java.time.Period] =
    SparkType[java.time.Period](YearMonthIntervalType())
}
