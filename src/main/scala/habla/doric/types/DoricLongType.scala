package habla.doric
package types

import habla.doric.syntax.NumericOperations

import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.Column

trait DoricLongType {

  type LongColumn = DoricColumn[Long]

  implicit val fromLong: FromDf[Long] = new FromDf[Long] {

    override def dataType: DataType = LongType

  }

  implicit val longArith: NumericOperations[Long] =
    new NumericOperations[Long] {}

  implicit val longCastToString: Casting[Long, String] =
    new SparkCasting[Long, String] {}

  implicit val longCastToFloat: Casting[Long, Float] =
    new SparkCasting[Long, Float] {}

  implicit val longCastToDouble: Casting[Long, Double] =
    new SparkCasting[Long, Double] {}

  object LongColumn {

    def apply(litv: Long): LongColumn =
      litv.lit

    def unapply(column: Column): Option[LongColumn] =
      DoricColumnExtr.unapply[Long](column)

  }
}
