package doric
package types

import java.sql.{Date, Timestamp}
import java.time.Instant

trait DateType[T] {}

object DateType {

  def apply[T]: DateType[T] = new DateType[T] {}

  implicit val dateCol: DateType[Date] = DateType[Date]

  implicit val localdateOps: DateType[Instant] = DateType[Instant]

  implicit val timestampDateOps: DateType[Timestamp] =
    DateType[Timestamp]

  implicit val instantDateOps: DateType[Instant] =
    DateType[Instant]
}
