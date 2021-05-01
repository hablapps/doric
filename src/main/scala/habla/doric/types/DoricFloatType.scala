package habla.doric
package types

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, FloatType}

trait DoricFloatType {

  type FloatColumn = DoricColumn[Float]

  object FloatColumn {

    def apply(litv: Float): FloatColumn =
      litv.lit

    def unapply(column: Column): Option[FloatColumn] =
      DoricColumnExtr.unapply[Float](column)
  }

  implicit val fromFloat: FromDf[Float] = new FromDf[Float] {

    override def dataType: DataType = FloatType
  }

  implicit val floatCastToString: Casting[Float, String] =
    new SparkCasting[Float, String] {}
  implicit val floatCastToDouble: Casting[Float, Double] =
    new SparkCasting[Float, Double] {}

}
