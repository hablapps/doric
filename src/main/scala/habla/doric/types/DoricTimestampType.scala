package habla.doric
package types

import habla.doric.syntax.{DateColumnLike, NumericOperations, TimestampColumnLike}
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.types.{DataType, TimestampType}
import org.apache.spark.sql.Column

trait DoricTimestampType {

  type TimestampColumn = DoricColumn[Timestamp]

  implicit val timestampDateOps: DateColumnLike[Timestamp] =
    new DateColumnLike[Timestamp] {}

  implicit val timestampOps: TimestampColumnLike[Timestamp] =
    new TimestampColumnLike[Timestamp] {}

  type InstantColumn = DoricColumn[Instant]

  implicit val instantOps: TimestampColumnLike[Instant] =
    new TimestampColumnLike[Instant] {}

  implicit val floatArith: NumericOperations[Float] =
    new NumericOperations[Float] {}

  implicit val fromTimestamp: FromDf[Timestamp] = new FromDf[Timestamp] {

    override def dataType: DataType = TimestampType

  }

  object TimestampColumn {

    def unapply(column: Column): Option[TimestampColumn] =
      DoricColumnExtr.unapply[Timestamp](column)
  }
}
