package habla.doric
package types

import java.time.Instant

import org.apache.spark.sql.types.{DataType, TimestampType}

trait DoricInstantType {

  implicit val fromInstant: FromDf[Instant] = new FromDf[Instant] {
    override def dataType: DataType = TimestampType
  }

}
