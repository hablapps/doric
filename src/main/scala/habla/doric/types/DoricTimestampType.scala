package habla.doric
package types

import habla.doric.syntax.{DateColumnLike, TimestampColumnLike}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.types.{DataType, TimestampType}
import org.apache.spark.sql.Column

trait DoricTimestampType {

  type TimestampColumn = DoricColumn[Timestamp]

  implicit val timestampDateOps: DateColumnLike[Timestamp] =
    new DateColumnLike[Timestamp] {}

  implicit val timestampOps: TimestampColumnLike[Timestamp] =
    new TimestampColumnLike[Timestamp] {}

  type InstantColumn = DoricColumn[Instant]

  implicit val instantDateOps: DateColumnLike[Instant] =
    new DateColumnLike[Instant] {}

  implicit val instantOps: TimestampColumnLike[Instant] =
    new TimestampColumnLike[Instant] {}

  implicit val fromTimestamp: SparkType[Timestamp] = new SparkType[Timestamp] {
    override def dataType: DataType = TimestampType
  }

  implicit val fromInstant: SparkType[Instant] = new SparkType[Instant] {
    override def dataType: DataType = TimestampType
  }

  object TimestampColumn {

    def unapply(column: Column): Option[TimestampColumn] =
      DoricColumnExtr.unapply[Timestamp](column)
  }

  implicit val timestampCastToInstant: Casting[Timestamp, Instant] =
    new SparkCasting[Timestamp, Instant] {}

  implicit val timestampCastToDate: Casting[Timestamp, Date] =
    new SparkCasting[Timestamp, Date] {}

  implicit val timestampCastToLocalDate: Casting[Timestamp, LocalDate] =
    new SparkCasting[Timestamp, LocalDate] {}
}
