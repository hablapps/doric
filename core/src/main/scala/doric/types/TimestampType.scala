package doric
package types

import java.sql.Timestamp
import java.time.Instant

trait TimestampType[T] {}

object TimestampType {

  def apply[T]: TimestampType[T] = new TimestampType[T] {}

  implicit val timestampOps: TimestampType[Timestamp] = TimestampType[Timestamp]

  implicit val instantOps: TimestampType[Instant] = TimestampType[Instant]
}
