package habla.doric
package types

import java.time.Instant

import org.apache.spark.sql.types.{DataType, TimestampType}

trait DoricInstantType {

  implicit val fromInstant: SparkType[Instant] = new SparkType[Instant] {
    override def dataType: DataType = TimestampType
  }

}
